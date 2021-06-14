package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 17:00
 * @Description:
 **/
object RDD_29_zip {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    val data1 = Array[String]("huangbo", "xuzheng", "wangbaoqiang")
    val data2 = Array[Int](18, 19, 20)

    val rdd1: RDD[String] = sc.parallelize(data1)
    val rdd2: RDD[Int] = sc.parallelize(data2)

    val resultRDD: RDD[(String, Int)] = rdd1.zip(rdd2)

    resultRDD.foreach(x => println(x))
  }
}
