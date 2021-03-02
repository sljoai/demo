package com.song.cn.agent.flink.state.twopc;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBConnectUtil {


    private static final ThreadLocal<Connection> connHolder = new ThreadLocal<>();

    private static Properties properties = new Properties();

    private static final BasicDataSource dataSource;


    static {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/mytable");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        /// 设置空闲和借用的连接的最大总数量，同时可以激活。
        dataSource.setMaxTotal(60);
        // 设置初始大小
        dataSource.setInitialSize(5);
        // 最小空闲连接
        dataSource.setMinIdle(8);
        // 最大空闲连接
        dataSource.setMaxIdle(16);
        // 超时等待时间毫秒
        dataSource.setMaxWaitMillis(2 * 10000);
        // 只会发现当前连接失效，再创建一个连接供当前查询使用
        dataSource.setTestOnBorrow(true);
        // removeAbandonedTimeout ：超过时间限制，回收没有用(废弃)的连接（默认为 300秒，调整为180）
        dataSource.setRemoveAbandonedTimeout(180);
        // removeAbandoned ：超过removeAbandonedTimeout时间后，是否进
        // 行没用连接（废弃）的回收（默认为false，调整为true)
        // DATA_SOURCE.setRemoveAbandonedOnMaintenance(removeAbandonedOnMaintenance);
        dataSource.setRemoveAbandonedOnBorrow(true);
        // testWhileIdle
        dataSource.setTestOnReturn(true);
        // testOnReturn
        dataSource.setTestOnReturn(true);
        // setRemoveAbandonedOnMaintenance
        dataSource.setRemoveAbandonedOnMaintenance(true);
        // 记录日志
        dataSource.setLogAbandoned(true);
        dataSource.setDefaultAutoCommit(true);
    }

    //加载DBCP配置文件
/*    static {
        try {
            FileInputStream fis = new FileInputStream(String.valueOf(DBConnectUtil.class.getResourceAsStream("dbcp.properties")));
            properties.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            try {
                dataSource = BasicDataSourceFactory.createDataSource(properties);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }*/

    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }


    public static Connection getConnection(String url, String username, String password) {
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }
}
