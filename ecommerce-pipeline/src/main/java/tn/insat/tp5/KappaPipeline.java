package tn.insat.tp5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;

import java.util.Iterator;

import static org.apache.spark.sql.functions.*;

/**
 * TP5 - Architecture Kappa (alternative a la Lambda)
 *
 * Dans l'architecture Kappa, on supprime la batch layer (HDFS) et on traite
 * TOUT via Spark Streaming. La rejoue des donnees historiques se fait en
 * conservant un historique long dans Kafka (retention = 30 jours par exemple).
 *
 * Avantages vs Lambda :
 *  - Un seul code a maintenir (pas de batch + streaming separes)
 *  - Moins de composants, moins de complexity operationnelle
 *  - La "source de verite" est Kafka, pas HDFS
 *
 * Inconvenients :
 *  - Necessite une retention Kafka longue (couteux en stockage)
 *  - Pas de stockage brut independant pour des analyses ad-hoc SQL
 *
 * Tables HBase utilisees :
 *  - 'sales'      : agregats par categorie (meme que pipeline Lambda)
 *  - 'events_raw' : TOUS les evenements bruts (remplace HDFS)
 *
 * Pour demarrer :
 *   hbase shell : create 'events_raw', 'data'
 *
 *   spark-submit --class tn.insat.tp5.KappaPipeline \
 *     --master local[4] --driver-memory 512m \
 *     --conf 'spark.sql.shuffle.partitions=4' \
 *     target/ecommerce-pipeline-1-jar-with-dependencies.jar
 */
public class KappaPipeline {

    static final String BOOTSTRAP_SERVERS  = "localhost:9092";
    static final String TOPIC              = "ecommerce-events";
    static final String HBASE_SALES        = "sales";
    static final String HBASE_EVENTS_RAW   = "events_raw";

    static StructType EVENT_SCHEMA = new StructType(new StructField[]{
        new StructField("user_id",    DataTypes.StringType, true, Metadata.empty()),
        new StructField("event_type", DataTypes.StringType, true, Metadata.empty()),
        new StructField("category",   DataTypes.StringType, true, Metadata.empty()),
        new StructField("amount",     DataTypes.DoubleType,  true, Metadata.empty()),
        new StructField("timestamp",  DataTypes.LongType,    true, Metadata.empty())
    });

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("KappaPipeline")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Source unique : un seul readStream pour les deux sinks (via foreachBatch)
        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> events = raw
                .selectExpr("CAST(value AS STRING) as json_str",
                             "CAST(offset AS STRING) as kafka_offset",
                             "CAST(partition AS STRING) as kafka_partition")
                .select(from_json(col("json_str"), EVENT_SCHEMA).as("data"),
                        col("kafka_offset"),
                        col("kafka_partition"))
                .select("data.*", "kafka_offset", "kafka_partition");

        // ---------------------------------------------------------------
        // Un seul foreachBatch gere les deux ecritures HBase :
        //   1) Tous les evenements bruts -> table 'events_raw'
        //   2) Achats agreges           -> table 'sales'
        // Pas de HDFS : Kafka est la source de verite pour le rejoue
        // ---------------------------------------------------------------
        StreamingQuery query = events.writeStream()
                .outputMode("append")
                .foreachBatch((batch, batchId) -> {
                    if (batch.isEmpty()) return;

                    System.out.println("=== [Kappa] Batch " + batchId
                            + " : " + batch.count() + " evenements ===");

                    Configuration conf = EcommercePipeline.buildHBaseConfig();

                    // --- Ecriture 1 : archivage brut dans 'events_raw' ---
                    batch.foreachPartition((Iterator<Row> rows) -> {
                        Configuration c = EcommercePipeline.buildHBaseConfig();
                        try (Connection conn = ConnectionFactory.createConnection(c);
                             Table rawTable = conn.getTable(TableName.valueOf(HBASE_EVENTS_RAW))) {

                            while (rows.hasNext()) {
                                Row row = rows.next();
                                String userId    = row.getString(row.fieldIndex("user_id"));
                                String evType    = row.getString(row.fieldIndex("event_type"));
                                String category  = row.getString(row.fieldIndex("category"));
                                double amount    = row.getDouble(row.fieldIndex("amount"));
                                long   ts        = row.getLong(row.fieldIndex("timestamp"));
                                String partition = row.getString(row.fieldIndex("kafka_partition"));
                                String offset    = row.getString(row.fieldIndex("kafka_offset"));

                                // RowKey = partition-offset pour garantir l'unicite
                                String rowKey = partition + "-" + offset;

                                Put put = new Put(Bytes.toBytes(rowKey));
                                put.addColumn(Bytes.toBytes("data"),
                                        Bytes.toBytes("user_id"), Bytes.toBytes(userId));
                                put.addColumn(Bytes.toBytes("data"),
                                        Bytes.toBytes("event_type"), Bytes.toBytes(evType));
                                put.addColumn(Bytes.toBytes("data"),
                                        Bytes.toBytes("category"), Bytes.toBytes(category));
                                // Montant chiffre
                                String encAmount;
                                try {
                                    encAmount = AESUtil.encrypt(String.valueOf(amount));
                                } catch (Exception e) {
                                    encAmount = String.valueOf(amount);
                                }
                                put.addColumn(Bytes.toBytes("data"),
                                        Bytes.toBytes("amount"), Bytes.toBytes(encAmount));
                                put.addColumn(Bytes.toBytes("data"),
                                        Bytes.toBytes("timestamp"), Bytes.toBytes(String.valueOf(ts)));
                                rawTable.put(put);
                            }
                        }
                    });

                    // --- Ecriture 2 : agregats des achats dans 'sales' ---
                    Dataset<Row> purchases = batch
                            .filter(col("event_type").equalTo("purchase"))
                            .groupBy(col("category"))
                            .agg(
                                    round(sum("amount"), 2).as("total_amount"),
                                    count("*").as("purchase_count")
                            );

                    long purchasesCount = purchases.count();
                    if (purchasesCount > 0) {
                        System.out.println("  -> " + purchasesCount + " categories avec achats");
                        purchases.show(false);

                        purchases.foreachPartition((Iterator<Row> rows) -> {
                            Configuration c = EcommercePipeline.buildHBaseConfig();
                            try (Connection conn = ConnectionFactory.createConnection(c);
                                 Table salesTable = conn.getTable(TableName.valueOf(HBASE_SALES))) {

                                while (rows.hasNext()) {
                                    Row row = rows.next();
                                    String cat   = row.getString(row.fieldIndex("category"));
                                    double total = row.getDouble(row.fieldIndex("total_amount"));
                                    long   cnt   = row.getLong(row.fieldIndex("purchase_count"));

                                    String encTotal;
                                    try {
                                        encTotal = AESUtil.encrypt(String.valueOf(total));
                                    } catch (Exception e) {
                                        encTotal = String.valueOf(total);
                                    }

                                    Put put = new Put(Bytes.toBytes(cat));
                                    put.addColumn(Bytes.toBytes("stats"),
                                            Bytes.toBytes("total_amount"), Bytes.toBytes(encTotal));
                                    put.addColumn(Bytes.toBytes("stats"),
                                            Bytes.toBytes("total_amount_plain"),
                                            Bytes.toBytes(String.valueOf(total)));
                                    put.addColumn(Bytes.toBytes("stats"),
                                            Bytes.toBytes("purchase_count"),
                                            Bytes.toBytes(String.valueOf(cnt)));
                                    put.addColumn(Bytes.toBytes("meta"),
                                            Bytes.toBytes("last_update"),
                                            Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
                                    salesTable.put(put);

                                    // Detection anomalie
                                    EcommercePipeline.detectAnomaly(cat, total, batchId);
                                    EcommercePipeline.previousCA.put(cat, total);
                                }
                            }
                        });
                    }

                    System.out.println("  -> events_raw: " + batch.count() + " lignes archivees dans HBase");
                })
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .option("checkpointLocation", "/tmp/checkpoint-kappa")
                .start();

        System.out.println("=== Architecture Kappa demarree ===");
        System.out.println(">>> Source unique Kafka -> [HBase events_raw] + [HBase sales]");
        System.out.println(">>> Pas de HDFS : Kafka est la source de verite (retention longue)");

        query.awaitTermination();
    }
}
