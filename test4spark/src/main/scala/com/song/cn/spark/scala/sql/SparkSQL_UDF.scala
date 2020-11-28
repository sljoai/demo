package com.song.cn.spark.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkSQL_UDF")

    val spark:SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()


    // 注册函数
    spark.udf.register("addName",(x:String) =>"Name: "+x)

    // 读取数据
    val dataFrame = spark.read.json("test4spark/input/atguigu/employees.json")
    dataFrame.createOrReplaceTempView("employees")
    dataFrame.show()

    // 查询
    val result = spark.sql("select addName(name) as name from employees")
    result.show()

    // 释放资源
    spark.stop()

  }

}
