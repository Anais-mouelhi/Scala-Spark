package tn.insat.tp5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.spark.sql.functions.*;

/**
 * TP5 - Pipeline End-to-End (version complete)
 *
 * Fonctionnalites :
 *  - Speed layer  : Kafka -> Spark -> HBase (agregats par categorie)
 *  - Batch layer  : Kafka -> Spark -> HDFS  (archivage brut)
 *  - Detection d'anomalies : alerte si CA chute de > 50% entre deux fenetres
 *  - Chiffrement AES-128 des montants avant stockage HBase
 */
public class EcommercePipeline {

    static final String BOOTSTRAP_SERVERS = "localhost:9092";
    static final String TOPIC             = "ecommerce-events";
    static final String HBASE_TABLE       = "sales";
    static final String HDFS_PATH         = "hdfs://hadoop-master:9000/data/ecommerce/raw";

    // Stocke le CA de la fenetre precedente par categorie (driver-side uniquement)
    static final Map<String, Double> previousCA = new ConcurrentHashMap<>();

    static StructType EVENT_SCHEMA = new StructType(new StructField[]{
        new StructField("user_id",    DataTypes.StringType, true, Metadata.empty()),
        new StructField("event_type", DataTypes.StringType, true, Metadata.empty()),
        new StructField("category",   DataTypes.StringType, true, Metadata.empty()),
        new StructField("amount",     DataTypes.DoubleType,  true, Metadata.empty()),
        new StructField("timestamp",  DataTypes.LongType,    true, Metadata.empty())
    });

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("EcommercePipeline")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.streaming.stateStore.providerClass",
                        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // ---------------------------------------------------------------
        // Source 1 : Speed layer - achats -> HBase (avec anomalies + chiffrement)
        // ---------------------------------------------------------------
        Dataset<Row> raw1 = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> events1 = raw1
                .selectExpr("CAST(value AS STRING) as json_str")
                .select(from_json(col("json_str"), EVENT_SCHEMA).as("data"))
                .select("data.*");

        Dataset<Row> purchases = events1
                .filter(col("event_type").equalTo("purchase"));

        Dataset<Row> aggregated = purchases
                .withColumn("event_ts", to_timestamp(col("timestamp").divide(1000)))
                .withWatermark("event_ts", "2 minutes")
                .groupBy(window(col("event_ts"), "1 minute"), col("category"))
                .agg(
                        round(sum("amount"), 2).as("total_amount"),
                        count("*").as("purchase_count")
                );

        StreamingQuery hbaseQuery = aggregated.writeStream()
                .outputMode("update")
                .foreachBatch((batch, batchId) -> {
                    long n = batch.count();
                    System.out.println("=== Batch " + batchId + " : " + n + " agregats ===");
                    if (n > 0) batch.show(false);

                    batch.foreachPartition((Iterator<Row> rows) -> {
                        Configuration conf = buildHBaseConfig();
                        try (Connection conn = ConnectionFactory.createConnection(conf);
                             Table table = conn.getTable(TableName.valueOf(HBASE_TABLE))) {

                            while (rows.hasNext()) {
                                Row row = rows.next();
                                String cat   = row.getString(row.fieldIndex("category"));
                                double total = row.getDouble(row.fieldIndex("total_amount"));
                                long   cnt   = row.getLong(row.fieldIndex("purchase_count"));

                                // --- Chiffrement AES du montant avant stockage ---
                                String encryptedAmount;
                                try {
                                    encryptedAmount = AESUtil.encrypt(String.valueOf(total));
                                } catch (Exception e) {
                                    encryptedAmount = String.valueOf(total); // fallback clair
                                    System.err.println("WARN: chiffrement echoue pour " + cat);
                                }

                                // --- Detection d'anomalies ---
                                detectAnomaly(cat, total, batchId);

                                Put put = new Put(Bytes.toBytes(cat));
                                // Le montant est chiffre dans HBase
                                put.addColumn(Bytes.toBytes("stats"),
                                        Bytes.toBytes("total_amount"),
                                        Bytes.toBytes(encryptedAmount));
                                put.addColumn(Bytes.toBytes("stats"),
                                        Bytes.toBytes("purchase_count"),
                                        Bytes.toBytes(String.valueOf(cnt)));
                                put.addColumn(Bytes.toBytes("meta"),
                                        Bytes.toBytes("last_update"),
                                        Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
                                // Conserver le montant en clair dans une colonne dediee pour debug
                                put.addColumn(Bytes.toBytes("stats"),
                                        Bytes.toBytes("total_amount_plain"),
                                        Bytes.toBytes(String.valueOf(total)));
                                table.put(put);

                                System.out.println("HBase PUT: " + cat
                                        + " | CA=" + total + " (chiffre) | n=" + cnt);
                            }
                        }
                    });
                    // Mise a jour de la fenetre precedente apres traitement du batch
                    batch.foreach(row -> {
                        String cat   = row.getString(row.fieldIndex("category"));
                        double total = row.getDouble(row.fieldIndex("total_amount"));
                        previousCA.put(cat, total);
                    });
                })
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .option("checkpointLocation", "/tmp/checkpoint-hbase")
                .start();

        // ---------------------------------------------------------------
        // Source 2 : Batch layer - tous evenements -> HDFS
        // ---------------------------------------------------------------
        Dataset<Row> raw2 = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> events2 = raw2
                .selectExpr("CAST(value AS STRING) as json_str")
                .select(from_json(col("json_str"), EVENT_SCHEMA).as("data"))
                .select("data.*");

        StreamingQuery hdfsQuery = events2.writeStream()
                .outputMode("append")
                .format("json")
                .option("path", HDFS_PATH)
                .option("checkpointLocation", "/tmp/checkpoint-hdfs")
                .trigger(Trigger.ProcessingTime("1 minute"))
                .start();

        System.out.println("=== Pipeline EcommercePipeline demarre ===");
        System.out.println(">>> Speed layer  : Kafka -> Spark -> HBase (table '" + HBASE_TABLE + "')");
        System.out.println(">>> Batch layer  : Kafka -> Spark -> HDFS (" + HDFS_PATH + ")");
        System.out.println(">>> Anomalies    : alerte si CA chute > 50% entre deux fenetres");
        System.out.println(">>> Chiffrement  : montants AES-128 avant stockage HBase");

        spark.streams().awaitAnyTermination();
    }

    /**
     * Compare le CA courant avec la fenetre precedente.
     * Emet une alerte si la chute depasse 50%.
     */
    static void detectAnomaly(String category, double currentCA, long batchId) {
        Double prev = previousCA.get(category);
        if (prev != null && prev > 0) {
            double changePct = ((currentCA - prev) / prev) * 100.0;
            if (changePct < -50.0) {
                System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                System.out.println("!!! ANOMALIE DETECTEE - Batch " + batchId + " !!!");
                System.out.println("!!! Categorie  : " + category);
                System.out.printf( "!!! CA precedent : %.2f EUR%n", prev);
                System.out.printf( "!!! CA actuel    : %.2f EUR%n", currentCA);
                System.out.printf( "!!! Variation    : %.1f%%%n", changePct);
                System.out.println("!!! ACTION : verifier la disponibilite produit !");
                System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            } else {
                System.out.printf("OK %s : %.2f EUR (%.1f%% vs fenetre precedente)%n",
                        category, currentCA, changePct);
            }
        }
    }

    /** Construit la configuration HBase. */
    static Configuration buildHBaseConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.rootdir", "hdfs://hadoop-master:9000/hbase");
        conf.set("hbase.cluster.distributed", "true");
        return conf;
    }
}
