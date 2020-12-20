package com.song.cn.jvm.classloader;

import java.sql.DriverManager;

public class MyTest31 {
    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        DriverManager.getConnection("jdbc:mysql://localhost:3306/mytestdb",
                "username",
                "password");
    }
}
