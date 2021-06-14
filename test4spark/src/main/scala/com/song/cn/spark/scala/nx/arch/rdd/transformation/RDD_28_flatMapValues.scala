package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 18:10
 * @Description:
 **/
object RDD_28_flatMapValues {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val rdd2 = rdd.map(x => (x.length, x))
    rdd2.foreach(x => println(x))

    /**
     * TODO
     */
    rdd2.flatMapValues("x" + _ + "x").foreach(println)
  }
}
