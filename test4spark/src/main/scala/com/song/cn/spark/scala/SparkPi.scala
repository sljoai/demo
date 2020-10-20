package com.song.cn.spark.scala

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.random

object SparkPi {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
    val spark = new SparkContext(conf)
    val slices = 2;
    val n = math.min(10000L * slices, Int.MaxValue).toInt
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)
    print(s"Pi is roughly ${4.0 * count / (n - 1)}")
  }
}
