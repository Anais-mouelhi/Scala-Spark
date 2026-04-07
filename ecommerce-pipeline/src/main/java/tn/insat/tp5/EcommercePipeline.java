package tn.insat.tp5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;          // org.apache.spark.sql.Row (pas hbase)
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;

import java.util.Iterator;

import static org.apache.spark.sql.functions.*;

/**
 * TP5 - Pipeline End-to-End
 *
 * Architecture Lambda :
 *   Kafka (ecommerce-events)
 *       |---- Spark Structured Streaming ---> HBase (sales)   [speed layer]
 *       |---- Spark writeStream json      ---> HDFS (raw)     [batch layer]
 */
public class EcommercePipeline {

    static final String BOOTSTRAP_SERVERS = "localhost:9092";
    static final String TOPIC             = "ecommerce-events";
    static final String HBASE_TABLE       = "sales";
    static final String HDFS_PATH         = "hdfs://hadoop-master:9000/data/ecommerce/raw";

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
                // Reduire les partitions de shuffle pour economiser la memoire
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.streaming.stateStore.providerClass",
                        "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // ---------------------------------------------------------------
        // Source 1 : Speed layer (purchases -> HBase)
        // Kafka source independante pour eviter les conflits de consumer group
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
                        Configuration conf = HBaseConfiguration.create();
                        conf.set("hbase.zookeeper.quorum", "localhost");
                        conf.set("hbase.zookeeper.property.clientPort", "2181");
                        conf.set("hbase.rootdir", "hdfs://hadoop-master:9000/hbase");
                        conf.set("hbase.cluster.distributed", "true");

                        try (Connection conn = ConnectionFactory.createConnection(conf);
                             Table table = conn.getTable(TableName.valueOf(HBASE_TABLE))) {

                            while (rows.hasNext()) {
                                Row row = rows.next();
                                String cat   = row.getString(row.fieldIndex("category"));
                                double total = row.getDouble(row.fieldIndex("total_amount"));
                                long   cnt   = row.getLong(row.fieldIndex("purchase_count"));

                                Put put = new Put(Bytes.toBytes(cat));
                                put.addColumn(Bytes.toBytes("stats"),
                                        Bytes.toBytes("total_amount"),
                                        Bytes.toBytes(String.valueOf(total)));
                                put.addColumn(Bytes.toBytes("stats"),
                                        Bytes.toBytes("purchase_count"),
                                        Bytes.toBytes(String.valueOf(cnt)));
                                put.addColumn(Bytes.toBytes("meta"),
                                        Bytes.toBytes("last_update"),
                                        Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
                                table.put(put);
                                System.out.println("HBase PUT: " + cat
                                        + " | CA=" + total + " | n=" + cnt);
                            }
                        }
                    });
                })
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .option("checkpointLocation", "/tmp/checkpoint-hbase")
                .start();

        // ---------------------------------------------------------------
        // Source 2 : Batch layer (tous evenements -> HDFS)
        // Deuxieme source Kafka independante
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
        System.out.println(">>> Speed layer : Kafka -> Spark -> HBase (table 'sales')");
        System.out.println(">>> Batch layer : Kafka -> Spark -> HDFS (" + HDFS_PATH + ")");

        spark.streams().awaitAnyTermination();
    }
}
