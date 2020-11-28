package com.song.cn.spark.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

/**
 * 通过继承 UserDefinedAggregateFunction 实现自定义UDF的方式操作DataFrame
 */
object SparkSQL_UDAF1  {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SparkSQL_UDAF1")

    val spark:SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // 注册函数
    spark.udf.register("myAverage",new MySalaryAverage)

    // 读取数据
    val dataFrame = spark.read.json("test4spark/input/atguigu/employees.json")
    dataFrame.createOrReplaceTempView("employees")
    dataFrame.show()

    // 查询
    val result = spark.sql("select myAverage(salary) as average_salary from employees")
    result.show()

    // 释放资源
    spark.stop()
  }

}

/**
 * 声明用户自定义聚合函数
 */
class MySalaryAverage extends UserDefinedAggregateFunction{
  // 聚合函数输入参数的数据类型
  override def inputSchema: StructType = /*StructType(StructField("inputColumn",LongType)::Nil)*/{
    new StructType().add("age",LongType)
  }

  // 聚合缓冲区中值的数据类型
  override def bufferSchema: StructType =/*{
    StructType(StructField("sum",LongType)::StructField("count",LongType)::Nil)
  }*/
  {
    new StructType().add("sum",LongType)
      .add("count",LongType)
  }

  // 返回值的数据类型
  override def dataType: DataType = DoubleType

  // 对于相同的输入是否一直返回相同的输出: 函数是否稳定
  override def deterministic: Boolean = true

  // 初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 存工资的总额
    buffer(0) = 0L
    // 存工资的个数
    buffer(1) = 0L
  }

  // 相同Execute间的数据合并: 根据查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)){
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 不同Execute间的数据合并: 将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    // count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}
