package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:46
 * @Description: TODO
 **/
object RDD_11_foldByKey {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    /**
     * 第一个案例
     */
    val rdd = sc.parallelize(List("dog", "apple", "book", "pig", "desk"), 2)
    val rdd2 = rdd.map(x => (x.length, x))
    val resultRDD: RDD[(Int, String)] = rdd2.foldByKey("")(_ + _)
    resultRDD.foreach(println)
  }
}
