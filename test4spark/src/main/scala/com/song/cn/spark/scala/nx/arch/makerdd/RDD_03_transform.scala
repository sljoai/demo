package com.song.cn.spark.scala.nx.arch.makerdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/18 10:50
 * @Description: 从原有的rdd进行转化
 **/
object RDD_03_transform {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    /**
     * 第一波
     */
    val array1 = Array("hello huangbo", "hello xuzheng", "hello wangbaoqiang")
    val lineRDD: RDD[String] = sc.parallelize(array1, 3)

    // 转化得来
    val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))
  }
}
