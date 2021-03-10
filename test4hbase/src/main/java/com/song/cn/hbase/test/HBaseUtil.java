package com.song.cn.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {

    private static ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();

    /**
     * 获取 HBase 连接对象
     *
     * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {
        Connection connection = connectionThreadLocal.get();

        if (connection == null) {
            Configuration configuration = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(configuration);
            connectionThreadLocal.set(connection);
        }
        return connection;
    }

    /**
     * 关闭 HBase 连接
     *
     * @throws IOException
     */
    public static void close() throws IOException {
        Connection connection = connectionThreadLocal.get();
        if (connection != null) {
            connection.close();
            connectionThreadLocal.remove();
        }
    }

    public static void insertData(String tableName,
                                  String rowKey,
                                  String family,
                                  String column,
                                  String value
    ) throws IOException {
        // 创建TableName
        TableName student = TableName.valueOf(tableName);

        // 获取指定的表
        Table table = connectionThreadLocal.get()
                .getTable(student);

        // 插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family),
                Bytes.toBytes(column),
                Bytes.toBytes(value));
        table.put(put);
    }

    /**
     * 获取分区键
     *
     * @param regionCount 分区数量
     * @return 分区键字节数组
     */
    public static byte[][] getRegionKeys(int regionCount) {
        byte[][] bs = new byte[regionCount][];

        for (int i = 0; i < regionCount; i++) {
            bs[i] = Bytes.toBytes(i + "|");
        }
        return bs;
    }

    /**
     * 根据rowKey和分区号，获取主键号
     * @param rowKey
     * @param regionCount
     * @return
     */
    public static String getRegionName(String rowKey, int regionCount) {
        int regionNum;
        int hash = rowKey.hashCode();

        if (regionCount > 0 && (regionCount & (regionCount - 1)) == 0) {
            regionNum = hash & (regionCount - 1);
        } else {
            regionNum = hash % regionCount;
        }
        return regionNum + "_" + rowKey;
    }
}
