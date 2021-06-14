package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:47
 * @Description: TODO
 **/
object RDD_12_sortByKey {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    sortByKey(sc)
  }

  def sortByKey(sc: SparkContext): Unit = {
    val list = List((99, "张三丰"), (96, "东方不败"), (66, "林平之"), (98, "聂风"))
    sc.parallelize(list).sortByKey(false).foreach(tuple => println(tuple._2 + "->" + tuple._1))
  }
}
