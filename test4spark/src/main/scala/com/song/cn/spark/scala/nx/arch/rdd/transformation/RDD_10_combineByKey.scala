package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:47
 * @Description: TODO
 **/
object RDD_10_combineByKey {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    val names = List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee")
    val values = List(1, 1, 2, 2, 2, 1, 2, 2, 2)
    val rdd1 = sc.parallelize(names, 3)
    val rdd2 = sc.parallelize(values, 3)

    val rdd3: RDD[(Int, String)] = rdd2.zip(rdd1)

    /**
     * 三个参数：
     * 1、createCombiner: V => C,
     * 2、mergeValue: (C, V) => C,
     * 3、mergeCombiners: (C, C) => C)
     */
    val resultRDD = rdd3.combineByKey(List(_),
      (x: List[String], y: String) => y :: x,
      (x: List[String], y: List[String]) => x ::: y
    )

    // 打印 RDD
    resultRDD.foreach(println)
  }

  def combineByKey(sc: SparkContext): Unit = {
    val textRDD: RDD[(String, String, Int)] = sc.parallelize(List(
      ("huangbo", "math", 35), ("huangbo", "english", 98),
      ("xuzheng", "math", 55), ("xuzheng", "english", 72), ("xuzheng", "java", 80),
      ("wangbaoqiang", "math", 77), ("wangbaoqiang", "english", 87), ("wangbaoqiang", "scala", 67)

    ))
    val keyValueRDD: RDD[(String, (String, Int))] = textRDD.map(x => {
      (x._1, (x._2, x._3))
    })
    val resultRDD = keyValueRDD.combineByKey(
      (value: (String, Int)) => (value._2, 1),
      (c: (Int, Int), v: (String, Int)) => (c._1 + v._2, c._2 + 1),
      (c: (Int, Int), v: (Int, Int)) => (c._1 + v._1, c._2 + v._2)
    )
    resultRDD.foreach(x => {
      println(x._1, x._2._1 * 1D / x._2._2)
    })
  }
}
