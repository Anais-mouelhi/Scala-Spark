package tn.insat.tp4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HelloHBase {
    private Table table1;
    private String tableName = "user";
    private String family1 = "PersonalData";
    private String family2 = "ProfessionalData";

    public void createHbaseTable() throws IOException {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()) {
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family1))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family2))
                    .build();
            System.out.println("Connecting");
            System.out.println("Creating Table");
            createOrOverwrite(admin, tableDescriptor);
            System.out.println("Done......");
            table1 = connection.getTable(TableName.valueOf(tableName));
            try {
                System.out.println("Adding user: user1");
                byte[] row1 = Bytes.toBytes("user1");
                Put p = new Put(row1);
                p.addColumn(Bytes.toBytes(family1), Bytes.toBytes("name"), Bytes.toBytes("ahmed"));
                p.addColumn(Bytes.toBytes(family1), Bytes.toBytes("address"), Bytes.toBytes("tunis"));
                p.addColumn(Bytes.toBytes(family2), Bytes.toBytes("company"), Bytes.toBytes("biat"));
                p.addColumn(Bytes.toBytes(family2), Bytes.toBytes("salary"), Bytes.toBytes("10000"));
                table1.put(p);
                System.out.println("Adding user: user2");
                byte[] row2 = Bytes.toBytes("user2");
                Put p2 = new Put(row2);
                p2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("name"), Bytes.toBytes("imen"));
                p2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("tel"), Bytes.toBytes("21212121"));
                p2.addColumn(Bytes.toBytes(family2), Bytes.toBytes("profession"), Bytes.toBytes("educator"));
                p2.addColumn(Bytes.toBytes(family2), Bytes.toBytes("company"), Bytes.toBytes("insat"));
                table1.put(p2);
                System.out.println("Reading data...");
                Get g = new Get(row1);
                Result r = table1.get(g);
                System.out.println(Bytes.toString(r.getValue(Bytes.toBytes(family1), Bytes.toBytes("name"))));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            if (table1 != null) {
                table1.close();
            }
        }
    }

    public static void createOrOverwrite(Admin admin, TableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void main(String[] args) throws IOException {
        HelloHBase admin = new HelloHBase();
        admin.createHbaseTable();
    }
}
