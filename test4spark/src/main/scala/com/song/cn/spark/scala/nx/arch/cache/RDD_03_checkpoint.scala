package com.song.cn.spark.scala.nx.arch.cache

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 15:27
 * @Description:
 **/
object RDD_03_checkpoint {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "bigdata")

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    // 构造RDD
    val list1 = List(4, 5, 6, 7, 8, 9, 10)
    val rdd = sc.parallelize(list1)

    sc.setCheckpointDir("hdfs://hadoop277ha/spark_checkpoint_dir/output1111")

    rdd.checkpoint()

    val result: Long = rdd.count()

    println(result)
  }
}
