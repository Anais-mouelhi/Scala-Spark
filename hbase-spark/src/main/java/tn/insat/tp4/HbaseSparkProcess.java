package tn.insat.tp4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HbaseSparkProcess {

    private static final String ZK_QUORUM = "localhost";
    private static final String ZK_PORT = "2181";
    private static final String HBASE_ROOTDIR = "hdfs://hadoop-master:9000/hbase";
    private static final String TABLE_NAME = "sales_ledger";

    public void run() throws Exception {
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseTest")
                .setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // Collect row keys on the driver first, then distribute to Spark
        Configuration driverConfig = buildConfig();
        List<String> rowKeys = new ArrayList<>();
        try (Connection conn = ConnectionFactory.createConnection(driverConfig);
             Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
             ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result r : scanner) {
                rowKeys.add(Bytes.toString(r.getRow()));
            }
        }
        System.out.println("Row keys collectées: " + rowKeys);

        // Create an RDD from the row keys and read each row in parallel via Spark
        JavaRDD<String> keysRDD = jsc.parallelize(rowKeys, 4);

        JavaRDD<String> rowsRDD = keysRDD.mapPartitions(keys -> {
            Configuration partConfig = buildConfig();
            List<String> results = new ArrayList<>();
            try (Connection conn = ConnectionFactory.createConnection(partConfig);
                 Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {
                while (keys.hasNext()) {
                    String key = keys.next();
                    org.apache.hadoop.hbase.client.Get get =
                            new org.apache.hadoop.hbase.client.Get(Bytes.toBytes(key));
                    Result r = table.get(get);
                    results.add(key + " -> colonnes: " + r.size());
                }
            }
            return results.iterator();
        });

        System.out.println("nombre d'enregistrements: " + rowsRDD.count());

        jsc.close();
    }

    private static Configuration buildConfig() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ZK_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        config.set("hbase.rootdir", HBASE_ROOTDIR);
        config.set("hbase.cluster.distributed", "true");
        return config;
    }

    public static void main(String[] args) throws Exception {
        HbaseSparkProcess process = new HbaseSparkProcess();
        process.run();
    }
}
