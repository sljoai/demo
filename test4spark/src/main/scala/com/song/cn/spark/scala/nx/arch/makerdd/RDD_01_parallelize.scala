package com.song.cn.spark.scala.nx.arch.makerdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/18 10:44
 * @Description: parallelize 并行化
 **/
object RDD_01_parallelize {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    /**
     * 第一波
     */
    val list1 = List(1, 2, 3, 4, 5, 6, 7, 7, 7, 7, 7, 8)
    val array1 = Array(("a", 6), ("b", 5), ("d", 3), ("c", 4), ("e", 2), ("f", 1))
    val array2 = Array("hello huangbo", "hello xuzheng", "hello wangbaoqiang")
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    val rdd2: RDD[(String, Int)] = sc.parallelize(array1, 3)
    val rdd3: RDD[String] = sc.parallelize(array2, 3)


    val list2 = List(1, 2, 3, 4, 5, 6, 7, 7, 7, 7, 8, 7, 8)
    val dataRDD1: RDD[Int] = sc.parallelize(list2, 3)
  }
}
