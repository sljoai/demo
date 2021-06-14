package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:42
 * @Description: TODO
 **/
object RDD_17_intersection {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    // 构造RDD
    val rdd1 = sc.parallelize(List(5, 6, 4, 3))
    val rdd2 = sc.parallelize(List(1, 2, 3, 4))

    /**
     * 求交集:对源RDD和参数RDD求交集后返回一个新的RDD
     */
    val resultRDD = rdd1.intersection(rdd2)

    // 打印RDD
    resultRDD.foreach(println)
  }

  def intersection(sc: SparkContext): Unit = {
    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    list1RDD.intersection(list2RDD).foreach(println(_))
  }
}
