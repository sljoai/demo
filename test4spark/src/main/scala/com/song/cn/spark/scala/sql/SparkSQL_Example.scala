package com.song.cn.spark.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkSQL_Example {

  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkSQL01_Demo")

    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // 进行转换之前，需要引入隐式转换规则
    // 这里的spark 不是包名的含义，是SparkSession对象的名字
    import spark.implicits._

    val stockFilePath: String = "test4spark/input/atguigu/tbStock.txt"
    // 创建RDD
    val tbStockRdd = spark.sparkContext.textFile(stockFilePath)

    // RDD -> DS
    val tbStockDs = tbStockRdd.map(_.split(","))
      .map(attr => TbStock(attr(0), attr(1), attr(2)))
      .toDS()
    tbStockDs.show(5)

    val stockDetailFilePath: String = "test4spark/input/atguigu/tbStockDetail.txt"

    // 创建 RDD
    val tbStockDetailRdd = spark.sparkContext.textFile(stockDetailFilePath)

    // RDD -> DS
    val tbStockDetailDs = tbStockDetailRdd.map(_.split(","))
      .map(attr => TbStockDetail(attr(0),
        attr(1).trim.toInt,
        attr(2),
        attr(3).trim().toInt,
        attr(4).trim.toDouble,
        attr(5).trim.toDouble)).toDS()
    tbStockDetailDs.show(5)


    val tbDateFilePath = "test4spark/input/atguigu/tbDate.txt"
    // 创建 RDD
    val tbDateRdd = spark.sparkContext.textFile(tbDateFilePath)

    // RDD -> DS
    val tbDateDs = tbDateRdd.map(_.split(","))
      .map(
        attr => TbDate(
          attr(0),
          attr(1).trim().toInt,
          attr(2).trim().toInt,
          attr(3).trim().toInt,
          attr(4).trim().toInt,
          attr(5).trim().toInt,
          attr(6).trim().toInt,
          attr(7).trim().toInt,
          attr(8).trim().toInt)).toDS()
    tbDateDs.show(5)


    // 注册表
    tbStockDs.createOrReplaceTempView("tbStock")
    tbDateDs.createOrReplaceTempView("tbDate")
    tbDateDs.createOrReplaceTempView("tbStockDetail")


    // TODO: 计算所有订单中每年的销售单数、销售总额


    // 释放资源
    spark.stop()
  }
}

case class TbStock(orderNumber: String, //订单号
                   locationId: String, // 交易位置
                   dateId: String // 交易日期
                  )
  extends Serializable

case class TbStockDetail(orderNumber: String, // 订单号
                         rowNum: Int, // 行号
                         itemId: String, // 售品
                         number: Int, // 数量
                         price: Double, // 单价
                         amount: Double // 销售额
                        )
  extends Serializable

case class TbDate(dateId: String, // 日期ID
                  years: Int, // 年月
                  theYear: Int, // 年
                  month: Int, // 月
                  day: Int, // 日
                  weekday: Int, // 星期几
                  quarter: Int, // 季度
                  period: Int,  // 旬
                  halfMonth: Int  // 半月
                 )
  extends Serializable


