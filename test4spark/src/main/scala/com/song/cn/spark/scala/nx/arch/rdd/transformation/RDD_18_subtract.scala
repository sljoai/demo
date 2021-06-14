package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:43
 * @Description: TODO
 **/
object RDD_18_subtract {

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
     * 求差集:返回前rdd元素不在后rdd的rdd
     */
    val resultRDD = rdd1.subtract(rdd2)

    // 打印RDD
    resultRDD.foreach(println)
  }
}
