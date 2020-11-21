package com.song.cn.spark.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 1. RDD -> DataSet -> RDD
 */
object SparkSQL_Trannform2 {

  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkSQL01_Demo")

    val spark:SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // 进行转换之前，需要引入隐式转换规则
    // 这里的spark 不是包名的含义，是SparkSession对象的名字
    import spark.implicits._

    // 创建RDD
    val rdd: RDD[(Int,String,Int)]  = spark.sparkContext.makeRDD(
      List((1,"zhangsan",20),(2,"lisi",30),(3,"wangwu",40)))


    // RDD -> DataSet
    val userDS: Dataset[User] = rdd.map{
      case (id,name,age) => {
        User(id,name,age)
      }
    }.toDS()

    // DataSet -> RDD
    val  rdd1: RDD[User] = userDS.rdd

    rdd1.foreach(println)
    //User(1,zhangsan,20)
    //User(3,wangwu,40)
    //User(2,lisi,30)

    // 释放资源
    spark.stop()
  }
}
