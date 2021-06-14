package com.song.cn.spark.scala.nx.arch.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:49
 * @Description: TODO
 **/
object RDD_01_coalesce {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    def myfunc(index: Int, iter: Iterator[Int]): Iterator[String] = {
      iter.map(x => "[partID:" + index + ", val: " + x + "]")
    }

    // 构造RDD
    val list1 = List(4, 5, 6, 7, 8, 9, 10)

    val rdd = sc.parallelize(list1, 3)
    val rdd11 = rdd.mapPartitionsWithIndex(myfunc).collect
    rdd11.foreach(println)

    println("------------------------")

    // shuffle为false, 则多个分区直接变成一个分区。
    // shuffle为true，则多个父分区的数据，经过随机shuffle
    val rdd2: RDD[Int] = rdd.coalesce(2, false)

    val rdd3 = rdd2.mapPartitionsWithIndex(myfunc).collect
    rdd3.foreach(println)
  }

  def coalesce(sc: SparkContext): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    sc.parallelize(list, 3).coalesce(1).foreach(println(_))
  }
}
