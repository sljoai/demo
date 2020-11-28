package com.song.cn.spark.scala.sql

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_Hive {

  def main(args: Array[String]): Unit = {
    // 本地文件路径
    val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

    // 创建配置对象
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      // 设置 hive metastore db 存储路径
      .set("spark.sql.warehouse.dir",warehouseLocation)
      .setAppName("SparkSQL_Hive")

    // 创建 SparkSession 对象
    val spark:SparkSession = SparkSession.builder()
      .config(sparkConf)
      // 设置支持 hive 操作
      .enableHiveSupport()
      .getOrCreate()
  }
}
