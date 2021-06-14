package com.song.cn.spark.scala.nx.arch.broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/18 23:35
 * @Description:
 **/
object BroadCastTest001 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("BroadCastTest001").setMaster("local")

    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sc.parallelize(1 to 10)

    val number = 100

    val resultRDd: RDD[Int] = rdd1.map((x: Int) => x + number)

    resultRDd.foreach(x => println(x))

    /**
     * 这里面的部分代码在 Drvier 中运行，
     * 部分代码在 executor 中运行
     */
  }
}
