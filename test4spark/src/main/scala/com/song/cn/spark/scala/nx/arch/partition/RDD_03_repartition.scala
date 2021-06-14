package com.song.cn.spark.scala.nx.arch.partition

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:50
 * @Description: TODO
 **/
object RDD_03_repartition {

  def main(args: Array[String]): Unit = {
    // 初始化编程入口
    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    repartition(sc)
  }

  def repartition(sc: SparkContext): Unit = {
    val list = List(1, 2, 3, 4)
    val listRDD = sc.parallelize(list, 1)
    listRDD.repartition(2).foreach(println(_))
  }
}
