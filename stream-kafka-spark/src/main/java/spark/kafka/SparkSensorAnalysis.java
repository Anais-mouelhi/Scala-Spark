package spark.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import static org.apache.spark.sql.functions.*;

/**
 * Consumes sensor data (city,temperature,humidity) from Kafka topic "sensor-data",
 * computes per-city averages (Kafka -> Spark) and persists raw records to HDFS
 * (Kafka -> HDFS) for later batch processing.
 *
 * Usage: SparkSensorAnalysis <bootstrap-servers> <topic> <group-id>
 */
public class SparkSensorAnalysis {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: SparkSensorAnalysis <bootstrap-servers> <topic> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topic            = args[1];
        String groupId          = args[2];

        SparkSession spark = SparkSession.builder()
                .appName("SparkSensorAnalysis")
                .getOrCreate();

        // --- Read raw stream from Kafka ---
        Dataset<Row> rawDf = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("kafka.group.id", groupId)
                .option("startingOffsets", "latest")
                .load();

        // Extract value as string: "city,temperature,humidity"
        Dataset<Row> valueDf = rawDf.selectExpr("CAST(value AS STRING) AS raw_data");

        // Parse each field from the CSV-like message
        Dataset<Row> parsedDf = valueDf
                .withColumn("city",        split(col("raw_data"), ",").getItem(0))
                .withColumn("temperature", split(col("raw_data"), ",").getItem(1).cast("double"))
                .withColumn("humidity",    split(col("raw_data"), ",").getItem(2).cast("double"));

        // --- Query 1: Kafka -> Spark (real-time averages to console) ---
        StreamingQuery consoleQuery = parsedDf
                .groupBy("city")
                .agg(
                    round(avg("temperature"), 2).as("avg_temperature"),
                    round(avg("humidity"),    2).as("avg_humidity"),
                    count("*").as("nb_mesures")
                )
                .writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .option("checkpointLocation", "/tmp/checkpoint-console")
                .start();

        // --- Query 2: Kafka -> HDFS (raw data for batch processing) ---
        StreamingQuery hdfsQuery = valueDf
                .writeStream()
                .outputMode("append")
                .format("csv")
                .option("path", "hdfs://hadoop-master:9000/sensor-data/")
                .option("checkpointLocation", "/tmp/checkpoint-hdfs")
                .start();

        System.out.println("=== Spark Sensor Analysis started ===");
        System.out.println(">>> Kafka -> Spark : real-time averages on console");
        System.out.println(">>> Kafka -> HDFS  : raw data at hdfs://hadoop-master:9000/sensor-data/");

        spark.streams().awaitAnyTermination();
    }
}
