package com.song.cn.spark.scala.nx.arch.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 18:26
 * @Description:
 **/
object RDD_20_foreachPartition {

    def main(args: Array[String]): Unit = {

        // 初始化编程入口
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local")
        sparkConf.setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)

        val names = List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee")
        val rdd1 = sc.parallelize(names, 3)

        rdd1.foreachPartition(iter => {
            println("----------------------------")
            for (x <- iter) {
                println(x)
            }
        })
    }
}
