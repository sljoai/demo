package com.song.cn.spark.scala.nx.arch.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513
 * 日期： 2017年6月18日 上午8:30:47
 *
 * 描述： Scala版本的WordCount
 */
object WordCount_Local {

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "hadoop")
        // 创建一个SparkConf对象，并设置程序的名称
        val conf = new SparkConf().setAppName("WordCount").setMaster("local")

        // 创建一个SparkContext对象
        val sparkContext: SparkContext = new SparkContext(conf)

        // 读取HDFS上的文件构建一个RDD
        // val fileRDD = sparkContext.textFile("hdfs://myha01/wc/input")
        val fileRDD = sparkContext.textFile(args(0), 2)

        // 构建一个单词RDD
        val wordAndOneRDD = fileRDD.flatMap(_.split(" ")).map((_, 1))

        // 进行单词的聚合
        val resultRDD = wordAndOneRDD.reduceByKey(_ + _, 3)

        // 对resultRDD进行单词出现次数的降序排序
        val sortedRDD: RDD[(String, Int)] = resultRDD.sortBy(_._2, false)

        // 输出结果
        // sortedRDD.saveAsTextFile("hdfs://myha01/wc/output4")
        sortedRDD.saveAsTextFile(args(1))

        sparkContext.stop()
    }
}
