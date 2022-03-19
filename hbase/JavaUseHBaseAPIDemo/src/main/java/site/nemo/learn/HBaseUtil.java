package site.nemo.learn;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseUtil {

    private static Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    private static Configuration configuration = null;
    private static Connection conn = null;
    private static Admin admin = null;

    static {
        try {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.master", "127.0.0.1:60000");

            conn = ConnectionFactory.createConnection(configuration);

            admin = conn.getAdmin();
        } catch (IOException e) {
            logger.info("hbase util init error=", e);
        }
    }

    public static void createTable(String tableName, String... columnFamilyNames) throws IOException {
        if (columnFamilyNames.length <= 0 || StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("tableName or columnFamilyNames must not null");
        }

        if (admin.tableExists(TableName.valueOf(tableName))) {
            logger.info("Table already exists");
        } else {
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(tableName));
            for (String colF : columnFamilyNames) {
                ColumnFamilyDescriptor colfDescriptor = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(colF))
                        .build();
                tableDescriptorBuilder.setColumnFamily(colfDescriptor);
            }
            admin.createTable(tableDescriptorBuilder.build());
        }
    }

    public static boolean isTableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void dropTable(String tableName) throws IOException {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    public static void putData(String tableName, String rowKey, String columnFamilyName,
                                  String columnKey, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnKey), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    public static void getData(String tableName, String rowKey, String columnFamilyName,
                                  String columnKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (StringUtils.isEmpty(columnKey)) {
            get.addFamily(Bytes.toBytes(columnFamilyName));
        } else {
            get.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnKey));
        }

        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            logger.info("family:{}, qualifier:{}, value:{}", family, qualifier, value);
        }
        table.close();
    }

    public static void scanTable(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                logger.info("row:{}, family:{}, qualifier:{}, value:{}", row, family, qualifier, value);
            }
        }
        table.close();
    }
}
