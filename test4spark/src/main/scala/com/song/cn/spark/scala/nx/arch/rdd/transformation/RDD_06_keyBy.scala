package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/18 11:45
 * @Description: keyBy的作用：
 *               给rdd中的每个value，生成一个key，最后变成 key-value 形式
 *               RDD[value] ==> RDD[(key, value)]
 **/
object RDD_06_keyBy {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    /**
     * 第一个案例
     *
     */
    val rdd1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    val resultRDD: RDD[(Int, String)] = rdd1.keyBy(_.length)
    resultRDD.foreach(println)
  }
}
