package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:42
 * @Description: TODO
 **/
object RDD_16_union {

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
     * 求并集:对源RDD和参数RDD求并集后返回一个新的RDD
     */
    val resultRDD = rdd1.union(rdd2)
    resultRDD.foreach(println)

    // key-value 类型的测试
    val rdd3 = sc.parallelize(List(("tom", 1), ("jerry", 3), ("kitty", 2), ("shuke", 1)))
    val rdd4 = sc.parallelize(List(("jerry", 2), ("tom", 3), ("shuke", 2), ("kitty", 5)))
    val resultRDD2 = rdd3.union(rdd4)
    resultRDD2.foreach(println)
  }

  def union(sc: SparkContext): Unit = {
    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)
    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)
    rdd1.union(rdd2).foreach(println(_))
  }
}
