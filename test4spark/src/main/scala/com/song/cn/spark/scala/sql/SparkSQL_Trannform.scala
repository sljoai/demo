package com.song.cn.spark.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 1. RDD -> DataFrame -> DataSet -> DataFrame -> RDD
 */
object SparkSQL_Trannform {

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

    // RDD -> DataFrame
    val userDataFrame:DataFrame = rdd.toDF("id","name","age")

    // DataFrame -> DataSet
    val userDataSet:Dataset[User] = userDataFrame.as[User]

    // DataSet -> DataFrame
    val userDataFrame1:DataFrame = userDataSet.toDF()

    // DataFrame -> RDD
    val userRDD1:RDD[Row] = userDataFrame1.rdd

    userRDD1.foreach{user =>
      println("id:"+user.getInt(0)+",name:"+user.getString(1)+",age:"+user.getInt(2))
    }

    userRDD1.foreach(println)
    //[3,wangwu,40]
    //[1,zhangsan,20]
    //[2,lisi,30]

    // 释放资源
    spark.stop()
  }
}
// 样例类
case class User(id:Long,name:String,age:Int)
