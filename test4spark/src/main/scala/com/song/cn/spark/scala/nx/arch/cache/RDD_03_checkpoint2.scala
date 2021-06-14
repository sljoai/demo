package com.song.cn.spark.scala.nx.arch.cache

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/18 18:46
 * @Description:
 **/
object RDD_03_checkpoint2 {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val sparkConf = new SparkConf().setAppName("RDD_CacheTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val data = sparkContext.parallelize(1 to 100, 5)
    sparkContext.setCheckpointDir("hdfs://myha01/spark_chk01/")

    println(data.count)
    data.checkpoint

    val ch1 = sparkContext.parallelize(1 to 2)
    val ch2 = ch1.map(_.toString + "[" + System.currentTimeMillis + "]")
    val ch3 = ch1.map(_.toString + "[" + System.currentTimeMillis + "]")

    // 由于没有checkpoint，所以每次都重新计算
    println(ch2.collect.mkString(","))
    println(ch2.collect.mkString(","))
    println(ch2.collect.mkString(","))

    println("-----------------------------------")
    ch3.checkpoint
    // 由于有checkpoint操作，第一次正常计算，然后持久化到HDFS目录，第二次，第三次，...，就从HDFS中读取到这个RDD
    println(ch3.collect.mkString(","))
    println(ch3.collect.mkString(","))
    println(ch3.collect.mkString(","))
    println(ch3.collect.mkString(","))

  }
}
