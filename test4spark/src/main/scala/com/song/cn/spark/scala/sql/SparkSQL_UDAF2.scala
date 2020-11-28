package com.song.cn.spark.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

object SparkSQL_UDAF2 {

  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkSQL_UDAF2")

    val spark:SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    // 创建聚合函数对象
    val udaf = new MyAverage1

    // 将聚合函数转换为查询列
    val avgCol: TypedColumn[Employee,Double]  = udaf.toColumn.name("avgSalary")

    val dataFrame: DataFrame = spark.read.json("test4spark/input/atguigu/employees.json")

    val employeeDS: Dataset[Employee] = dataFrame.as[Employee]

    // 应用函数
    employeeDS.select(avgCol).show()


    // 释放资源
    spark.stop()

  }
}

case class Employee(name: String, salary: Long)

case class Average(var sum: Long, var count: Long)

/**
 * 继承Aggregator, 设定泛型
 */
class MyAverage1 extends Aggregator[Employee, Average, Double] {

  // 定义一个数据结构，保存工资综述和工资总个数，初始化为0
  def zero: Average = Average(0L,0L)

  //
  override def reduce(b: Average, a: Employee): Average = {
    b.sum = b.sum + a.salary
    b.count = b.count +1
    b
  }

  // 聚合不同 execute 的结果
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  // 计算输出
  override def finish(reduction: Average): Double = {
    reduction.sum.toDouble/ reduction.count
  }

  // 设定值类型的编码器，要转换成case 类
  // Encoders.product 是进行 scala 元组 和 case 类转换的编码器
  override def bufferEncoder: Encoder[Average] = Encoders.product

  // 设定最终输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
