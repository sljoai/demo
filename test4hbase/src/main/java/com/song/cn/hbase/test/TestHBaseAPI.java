package com.song.cn.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;

/**
 * 通过Java代码访问hbase数据库
 */
public class TestHBaseAPI {
    static {
        PropertyConfigurator.configure("conf/log4j.properties");
    }

    private static final int REGION_COUNT = 10;

    public static void main(String[] args) throws IOException {

        // 0）创建配置对象，获取hbase的连接
        // TODO: 是否还有其他的读取配置的方法
        Configuration configuration = HBaseConfiguration.create();

        // 1）获取hbase连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        // 2) 获取操作对象：Admin
        Admin admin = connection.getAdmin();

        // 3) 操作数据库：
        // 3-1) 判断命名空间
        NamespaceDescriptor namespaceDescriptor;
        try {
            // TODO: 比较特殊的操作，通过异常来判断是否需要创建NS
            admin.getNamespaceDescriptor("xxx");
        } catch (NamespaceNotFoundException e) {
            namespaceDescriptor = NamespaceDescriptor.create("xxx").build();
            admin.createNamespace(namespaceDescriptor);
        }

        // 3-2) 判断hbase中是否存在某张表
        TableName student = TableName.valueOf("student_rk");
        boolean exists = admin.tableExists(student);
        System.out.println(exists);

        if (exists) {
            // 获取指定的表
            Table table = connection.getTable(student);
            // 查询数据
            String rowKey = "1001";
            String realRowKey = HBaseUtil.getRegionName(rowKey,REGION_COUNT);

            // 字符编码
            Get get = new Get(Bytes.toBytes(realRowKey));

            Result result = table.get(get);
            boolean empty = result.isEmpty();
            if (empty) {
                // 新增数据
                Put put = new Put(Bytes.toBytes(realRowKey));
                String family = "info";
                String column = "name";
                String value = "张三";
                put.addColumn(Bytes.toBytes(family),
                        Bytes.toBytes(column),
                        Bytes.toBytes(value));
                table.put(put);
                System.out.println("增加数据...");
            } else {
                // 展示数据
                for (Cell cell : result.rawCells()) {
                    System.out.println("value: "
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println("family: "
                            + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("column: "
                            + Bytes.toString(CellUtil.cloneQualifier(cell)));
                }
            }
        } else {
            // 创建表

            // 创建表的描述对象
            HTableDescriptor hTableDescriptor = new HTableDescriptor(student);
            // 添加协处理器
            hTableDescriptor.addCoprocessor("com.song.cn.hbase.coprocessor.InsertStudentCoProcessor");
            // 增加列族
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");
            hTableDescriptor.addFamily(columnDescriptor);

            // 分配并创建分区键
            admin.createTable(hTableDescriptor,HBaseUtil.getRegionKeys(REGION_COUNT));
            System.out.println("表创建成功...");
        }

    }

}
