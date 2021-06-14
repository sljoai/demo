package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 10:46
 * @Description: 类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素）
 **/
object RDD_02_flatMap {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    // 构造RDD
    val array = Array("hello huangbo", "hello xuzheng", "hello wangbaoqiang")
    val rdd = sc.parallelize(array)

    // 需求
    val resultRDD = rdd.flatMap(x => x.split(" "))

    // 打印RDD
    resultRDD.foreach(println)

  }

  def flatMap(sc: SparkContext): Unit = {
    val list = List("张无忌 赵敏", "宋青书 周芷若")
    val listRDD = sc.parallelize(list)
    val nameRDD = listRDD.flatMap(line => line.split(" ")).map(name => "Hello " + name)
    nameRDD.foreach(name => println(name))
  }
}
