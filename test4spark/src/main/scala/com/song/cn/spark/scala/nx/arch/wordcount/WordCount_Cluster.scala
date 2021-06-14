package com.song.cn.spark.scala.nx.arch.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513
 * 日期： 2017年6月18日 上午8:30:47
 * 描述： Scala版本的WordCount
 */
object WordCount_Cluster {

  def main(args: Array[String]): Unit = {

    // 创建一个SparkConf对象，并设置程序的名称
    val conf = new SparkConf().setAppName("WordCount")
    conf.setMaster("spark://bigdata02:7077,bigdata04:7077")

    // 创建一个SparkContext对象
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 读取HDFS上的文件构建一个RDD
    // 返回了一个 HadoopRDD
    val fileRDD: RDD[String] = sc.textFile(args(0))

    // 构建一个单词RDD
    // 返回一个 MapPartitionsRDD
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))

    // 构建单词对
    // 返回一个 MapPartitionsRDD
    val wordAndOneRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))

    // 进行单词的聚合
    // 返回一个 ShuffledRDD
    val resultRDD = wordAndOneRDD.reduceByKey(_ + _)

    // 对resultRDD进行单词出现次数的降序排序，然后写出结果到HDFS
    resultRDD.sortBy(_._2, false).saveAsTextFile(args(1))
    //  resultRDD.sortBy(_._2, false).saveAsTextFile(args(1))

    sc.stop()
  }
}
