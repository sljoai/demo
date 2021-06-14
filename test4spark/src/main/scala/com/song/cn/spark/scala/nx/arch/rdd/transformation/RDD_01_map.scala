package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 10:45
 * @Description: 返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
 **/
object RDD_01_map {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    // 构造RDD
    val list1 = List(4, 5, 6, 7, 8, 9, 10)
    val rdd = sc.parallelize(list1)

    // 需求：全部 * 2
    val resultRDD = rdd.map(x => x * 2)

    // 打印RDD
    resultRDD.foreach(println)
  }

  def map(sc: SparkContext): Unit = {
    val list = List("张无忌", "赵敏", "周芷若")
    val listRDD = sc.parallelize(list)
    val nameRDD = listRDD.map(name => "Hello " + name)
    nameRDD.foreach(name => println(name))
  }
}
