package com.song.cn.flink.aguigu.sink

import com.alibaba.druid.pool.DruidDataSourceFactory
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

import java.sql.{Connection, PreparedStatement}
import java.util.Properties
import javax.sql.DataSource

class MyJdbcSink(sql: String) extends RichSinkFunction[Array[String]] {

  val driver = "com.mysql.jdbc.Driver"

  val url = "jdbc:mysql://hadoop01:3306/gmall?useSSL=false"

  val username = "root"

  val password = "123456"

  val maxActive = "20"

  var connection: Connection = null;

  override def open(parameters: Configuration): Unit = {
    val properties = new Properties()
    properties.put("driverClassName", driver)
    properties.put("url", url)
    properties.put("username", username)
    properties.put("password", password)
    properties.put("maxActive", maxActive)

    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(properties)
    connection = dataSource.getConnection()
  }

  override def invoke(values: Array[String]): Unit = {
    val ps: PreparedStatement = connection.prepareStatement(sql)
    println(values.mkString(","))
    for (i <- 0 until values.length) {
      ps.setObject(i+1, values(i))
    }
    ps.executeUpdate()
  }

  override def close(): Unit = {
    if(connection!=null){
      connection.close()
    }
  }
}
