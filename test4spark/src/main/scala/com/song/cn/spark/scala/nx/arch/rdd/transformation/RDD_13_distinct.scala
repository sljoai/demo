package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:44
 * @Description: TODO
 **/
object RDD_13_distinct {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    /**
     * 第一波测试
     */
    val list1 = List(4, 5, 6, 4, 8, 6, 9)
    val rdd = sc.parallelize(list1)
    val resultRDD = rdd.distinct()
    resultRDD.foreach(println)

    /**
     * 第二波测试
     */
    val rdd2 = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
    val resultRDD2: RDD[String] = rdd2.distinct()
    resultRDD2.foreach(println)
  }

  def distinct(sc: SparkContext): Unit = {
    val list = List(1, 1, 2, 2, 3, 3, 4, 5)
    sc.parallelize(list).distinct().foreach(println(_))
  }
}
