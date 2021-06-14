package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:33
 * @Description: 返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成
 **/
object RDD_05_filter {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    // 构造RDD
    val list1 = List(4, 5, 6, 7, 8, 9, 10)
    val rdd = sc.parallelize(list1)

    // 需求：获取元素大于5的所有元素
    val resultRDD1 = rdd.filter(x => if (x > 5) true else false)

    // 需求：获取所有偶数
    val resultRDD2 = rdd.filter(x => if (x % 2 == 0) true else false)

    // 打印RDD
    resultRDD1.foreach(println)
    resultRDD2.foreach(println)
  }

  def filter(sc: SparkContext): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val listRDD = sc.parallelize(list)
    listRDD.filter(num => num % 2 == 0).foreach(print(_))
  }
}
