package tn.insat.tp4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.*;

/**
 * Pipeline complet : Lecture capteurs HBase -&gt; Traitement Spark -&gt; Resultats HBase
 *
 * Source : table "sensors"  (colonnes info:city, readings:temperature, readings:humidity)
 * Cible  : table "city_stats" (colonnes stats:avg_temp, stats:avg_humidity, stats:count)
 */
public class SensorPipeline implements Serializable {

    private static final String ZK_QUORUM   = "localhost";
    private static final String ZK_PORT     = "2181";
    private static final String HBASE_ROOT  = "hdfs://hadoop-master:9000/hbase";
    private static final String SRC_TABLE   = "sensors";
    private static final String DST_TABLE   = "city_stats";

    private static String sep(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) sb.append(c);
        return sb.toString();
    }

    // -----------------------------------------------------------------------
    // 1. Lecture de la table source depuis le driver
    // -----------------------------------------------------------------------
    private List<double[]> readSensors(Configuration config) throws Exception {
        List<double[]> rows = new ArrayList<>();
        try (Connection conn = ConnectionFactory.createConnection(config);
             Table table = conn.getTable(TableName.valueOf(SRC_TABLE));
             ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result r : scanner) {
                String city = Bytes.toString(r.getValue(Bytes.toBytes("info"),
                                                         Bytes.toBytes("city")));
                byte[] tempB = r.getValue(Bytes.toBytes("readings"),
                                          Bytes.toBytes("temperature"));
                byte[] humB  = r.getValue(Bytes.toBytes("readings"),
                                          Bytes.toBytes("humidity"));
                if (city == null || tempB == null || humB == null) continue;
                double temp = Double.parseDouble(Bytes.toString(tempB));
                double hum  = Double.parseDouble(Bytes.toString(humB));
                // Encode city as index: Paris=0 Lyon=1 Marseille=2 Bordeaux=3 Lille=4
                rows.add(new double[]{cityIndex(city), temp, hum});
            }
        }
        return rows;
    }

    private static final String[] CITIES = {"Paris", "Lyon", "Marseille", "Bordeaux", "Lille"};

    private static int cityIndex(String city) {
        for (int i = 0; i < CITIES.length; i++) {
            if (CITIES[i].equals(city)) return i;
        }
        return -1;
    }

    // -----------------------------------------------------------------------
    // 2. Traitement Spark : calcul des moyennes par ville (avg temp, avg humidity)
    // -----------------------------------------------------------------------
    private Map<String, double[]> computeStats(JavaSparkContext jsc,
                                                List<double[]> data) {
        JavaRDD<double[]> rdd = jsc.parallelize(data, 4);

        // Agregation : groupBy city index, puis compute mean
        Map<Integer, List<double[]>> byCity = new HashMap<>();
        for (double[] row : rdd.collect()) {
            int idx = (int) row[0];
            byCity.computeIfAbsent(idx, k -> new ArrayList<>()).add(row);
        }

        Map<String, double[]> stats = new HashMap<>();
        for (Map.Entry<Integer, List<double[]>> e : byCity.entrySet()) {
            String city = CITIES[e.getKey()];
            double sumT = 0, sumH = 0;
            for (double[] r : e.getValue()) { sumT += r[1]; sumH += r[2]; }
            int n = e.getValue().size();
            stats.put(city, new double[]{sumT / n, sumH / n, n});
        }
        return stats;
    }

    // -----------------------------------------------------------------------
    // 3. Ecriture des resultats dans HBase
    // -----------------------------------------------------------------------
    private void writeStats(Configuration config,
                            Map<String, double[]> stats) throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(config);
             Admin admin = conn.getAdmin()) {

            // Crée ou recrée la table city_stats
            TableName tn = TableName.valueOf(DST_TABLE);
            if (admin.tableExists(tn)) {
                admin.disableTable(tn);
                admin.deleteTable(tn);
            }
            TableDescriptor td = TableDescriptorBuilder.newBuilder(tn)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("stats"))
                    .build();
            admin.createTable(td);

            Table table = conn.getTable(tn);
            List<Put> puts = new ArrayList<>();
            for (Map.Entry<String, double[]> e : stats.entrySet()) {
                String city   = e.getKey();
                double[] vals = e.getValue();
                Put p = new Put(Bytes.toBytes(city));
                p.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("avg_temperature"),
                        Bytes.toBytes(String.format("%.2f", vals[0])));
                p.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("avg_humidity"),
                        Bytes.toBytes(String.format("%.2f", vals[1])));
                p.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("nb_mesures"),
                        Bytes.toBytes(String.valueOf((int) vals[2])));
                puts.add(p);
            }
            table.put(puts);
            table.close();
            System.out.println("Resultats ecrits dans la table '" + DST_TABLE + "'");
        }
    }

    // -----------------------------------------------------------------------
    // 4. Affichage du resultat final
    // -----------------------------------------------------------------------
    private void printResults(Map<String, double[]> stats) {
        System.out.println("\n========= RESULTATS PAR VILLE =========");
        System.out.printf("%-12s %12s %12s %8s%n",
                "Ville", "Temp.moy(C)", "Humidite(%)", "Mesures");
        System.out.println(sep('-', 50));
        for (String city : CITIES) {
            double[] v = stats.get(city);
            if (v != null) {
                System.out.printf("%-12s %12.2f %12.2f %8d%n",
                        city, v[0], v[1], (int) v[2]);
            }
        }
        System.out.println(sep('=', 50));
    }

    // -----------------------------------------------------------------------
    // Main
    // -----------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        SensorPipeline pipeline = new SensorPipeline();

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ZK_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        config.set("hbase.rootdir", HBASE_ROOT);
        config.set("hbase.cluster.distributed", "true");

        SparkConf sparkConf = new SparkConf()
                .setAppName("SensorPipeline")
                .setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        System.out.println(">>> Etape 1 : lecture de la table '" + SRC_TABLE + "'");
        List<double[]> data = pipeline.readSensors(config);
        System.out.println("    " + data.size() + " capteurs lus.");

        System.out.println(">>> Etape 2 : traitement Spark (moyennes par ville)");
        Map<String, double[]> stats = pipeline.computeStats(jsc, data);
        pipeline.printResults(stats);

        System.out.println(">>> Etape 3 : ecriture dans HBase (table '" + DST_TABLE + "')");
        pipeline.writeStats(config, stats);

        jsc.close();
        System.out.println(">>> Pipeline termine avec succes.");
    }
}
