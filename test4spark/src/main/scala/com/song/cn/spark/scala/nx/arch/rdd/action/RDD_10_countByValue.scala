package com.song.cn.spark.scala.nx.arch.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 17:53
 * @Description:
 **/
object RDD_10_countByValue {

    def main(args: Array[String]): Unit = {

        // 初始化编程入口
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local")
        sparkConf.setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.parallelize(List((3, "Gnu"), (3, "Yak"), (5, "Mouse"), (3, "Dog")), 2)
        /**
         * 统计每个key出现的次数
         */
        val result: collection.Map[(Int, String), Long] = rdd1.countByValue()
        for (a <- result) {
            println(a._1, a._2)
        }

        println("------------------------------------------------")
        val rdd2 = sc.parallelize(List("dog", "pig", "cat", "cat", "dog", "cat"), 2)
        val result2: collection.Map[String, Long] = rdd2.countByValue()
        for (a <- result2) {
            println(a._1, a._2)
        }
    }
}
