package tn.insat.tp5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;

/**
 * TP5 - Alternative Cassandra (comparaison avec HBase)
 *
 * Ce fichier montre comment remplacer le sink HBase par Apache Cassandra
 * en utilisant le connecteur Spark-Cassandra (DataStax).
 *
 * ==========================================================================
 * PREREQUIS CASSANDRA
 * ==========================================================================
 * 1. Installer Cassandra dans le conteneur :
 *    apt-get install -y cassandra
 *    service cassandra start
 *
 * 2. Creer le keyspace et la table :
 *    cqlsh
 *    CREATE KEYSPACE ecommerce WITH replication = {
 *      'class': 'SimpleStrategy', 'replication_factor': 1
 *    };
 *    USE ecommerce;
 *    CREATE TABLE sales (
 *      category       text PRIMARY KEY,
 *      total_amount   double,
 *      purchase_count bigint,
 *      last_update    bigint
 *    );
 *
 * 3. Ajouter la dependance dans pom.xml :
 *    <dependency>
 *      <groupId>com.datastax.spark</groupId>
 *      <artifactId>spark-cassandra-connector_2.12</artifactId>
 *      <version>3.5.0</version>
 *    </dependency>
 *
 * 4. Lancer avec les options Cassandra :
 *    spark-submit \
 *      --class tn.insat.tp5.CassandraSink \
 *      --conf spark.cassandra.connection.host=localhost \
 *      --conf spark.cassandra.connection.port=9042 \
 *      target/ecommerce-pipeline-1-jar-with-dependencies.jar
 *
 * ==========================================================================
 * COMPARAISON HBase vs Cassandra
 * ==========================================================================
 *
 *  Critere            | HBase                     | Cassandra
 *  -------------------|---------------------------|---------------------------
 *  Modele de donnees  | Column families (NoSQL)   | Wide-row CQL (proche SQL)
 *  Langage de requete | Java API / HBase Shell     | CQL (Cassandra Query Language)
 *  Latence ecriture   | ~2-5 ms (single Put)      | ~1-3 ms (prepared statement)
 *  Throughput ecriture| Bon avec BufferedMutator  | Excellent (log-structured)
 *  Latence lecture    | ~1 ms (get par row key)   | ~1 ms (get par partition key)
 *  Integration Hadoop | Native (sur HDFS)         | Independant de Hadoop
 *  Schema             | Flexible, sans schema      | Schema obligatoire (CQL)
 *  Disponibilite      | SPOF possible (HMaster)   | Pas de SPOF (peer-to-peer)
 *  Complexite ops     | Haute (ZooKeeper, HMaster) | Moyenne
 *  Cas d'usage ideal  | Donnees sur HDFS, scan    | Haute dispo, ecriture massive
 *
 * Conclusion : Pour notre pipeline e-commerce en production avec haute disponibilite,
 * Cassandra serait un excellent choix. HBase est prefere quand l'infrastructure
 * Hadoop existante doit etre mutualisee.
 */
public class CassandraSink {

    static final String BOOTSTRAP_SERVERS = "localhost:9092";
    static final String TOPIC             = "ecommerce-events";
    static final String KEYSPACE          = "ecommerce";
    static final String TABLE             = "sales";

    static StructType EVENT_SCHEMA = new StructType(new StructField[]{
        new StructField("user_id",    DataTypes.StringType, true, Metadata.empty()),
        new StructField("event_type", DataTypes.StringType, true, Metadata.empty()),
        new StructField("category",   DataTypes.StringType, true, Metadata.empty()),
        new StructField("amount",     DataTypes.DoubleType,  true, Metadata.empty()),
        new StructField("timestamp",  DataTypes.LongType,    true, Metadata.empty())
    });

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("CassandraSink")
                .config("spark.sql.shuffle.partitions", "4")
                // Configuration du connecteur Cassandra
                .config("spark.cassandra.connection.host", "localhost")
                .config("spark.cassandra.connection.port", "9042")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> events = raw
                .selectExpr("CAST(value AS STRING) as json_str")
                .select(from_json(col("json_str"), EVENT_SCHEMA).as("data"))
                .select("data.*");

        Dataset<Row> purchases = events.filter(col("event_type").equalTo("purchase"));

        Dataset<Row> aggregated = purchases
                .withColumn("event_ts", to_timestamp(col("timestamp").divide(1000)))
                .withWatermark("event_ts", "2 minutes")
                .groupBy(window(col("event_ts"), "1 minute"), col("category"))
                .agg(
                        round(sum("amount"), 2).as("total_amount"),
                        count("*").as("purchase_count")
                )
                // Ajouter le timestamp de mise a jour
                .withColumn("last_update", lit(System.currentTimeMillis()));

        // Ecriture dans Cassandra via le connecteur DataStax
        // (necessie la dependance spark-cassandra-connector_2.12 dans pom.xml)
        StreamingQuery query = aggregated.writeStream()
                .outputMode("update")
                .foreachBatch((batch, batchId) -> {
                    if (batch.isEmpty()) return;
                    System.out.println("=== [Cassandra] Batch " + batchId
                            + " : " + batch.count() + " agregats ===");
                    batch.show(false);

                    // Ecriture dans Cassandra via le format cassandra
                    batch.select("category", "total_amount", "purchase_count", "last_update")
                         .write()
                         .format("org.apache.spark.sql.cassandra")
                         .option("keyspace", KEYSPACE)
                         .option("table", TABLE)
                         .mode("append")
                         .save();

                    System.out.println("  -> Ecrit dans Cassandra " + KEYSPACE + "." + TABLE);
                })
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .option("checkpointLocation", "/tmp/checkpoint-cassandra")
                .start();

        System.out.println("=== CassandraSink demarre ===");
        System.out.println(">>> Kafka -> Spark -> Cassandra (keyspace=" + KEYSPACE + ", table=" + TABLE + ")");

        query.awaitTermination();
    }
}
