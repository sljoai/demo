package com.song.cn.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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
}
