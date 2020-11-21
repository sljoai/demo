package com.song.cn.spark.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark SQL Demo
 */
object SparkSQL_SQL {

  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkSQL01_Demo")

    val spark:SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    //Exception in thread "main" org.apache.spark.sql.AnalysisException:
    // Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
    // referenced columns only include the internal corrupt record column
    // (named _corrupt_record by default). For example:
    // spark.read.schema(schema).json(file).filter($"_corrupt_record".isNotNull).count()
    // and spark.read.schema(schema).json(file).select("_corrupt_record").show()
    // 以上错误的主要原因是 user.json中一个json串包含多行
    val frame: DataFrame = spark.read
      .json("test4spark/input/atguigu/user.json")
    frame.show(10)

    // 将DataFrame转换为一张表
    frame.createOrReplaceTempView("user")

    // 采用 sql 的语法访问数据
    spark.sql("select * from user").show()

    // 释放资源
    spark.stop()
  }

}
