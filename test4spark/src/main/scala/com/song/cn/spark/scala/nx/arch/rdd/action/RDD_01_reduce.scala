package com.song.cn.spark.scala.nx.arch.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:26
 * @Description:
 **/
object RDD_01_reduce {

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
    val result = rdd.reduce((x, y) => x + y)
    // 打印RDD
    println(result)


    val f = (x: Int, y: Int) => {

      println("运行细节" + (x, y))
      if (y % 2 == 0) {
        x - y
      } else {
        x + y
      }
    }

    println(rdd.reduce(f))
  }

  def reduce(sc: SparkContext): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD = sc.parallelize(list)
    val result = listRDD.reduce((x, y) => x + y)
    println(result)
    val maxValue = listRDD.reduce((x: Int, y: Int) => {
      if (x > y) x else y
    })
    println(maxValue)
  }
}
