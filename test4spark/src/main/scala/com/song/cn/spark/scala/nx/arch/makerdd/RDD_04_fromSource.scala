package com.song.cn.spark.scala.nx.arch.makerdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/18 10:54
 * @Description: 从外部数据源
 **/
object RDD_04_fromSource {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    /**
     * 引用文件系统的一个目录/文件
     */
    val lineRDD: RDD[String] = sc.textFile("hdfs://hadoop277ha/sparkwc/input/")

    lineRDD.foreach(line => println(line))
  }
}
