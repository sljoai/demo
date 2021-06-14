package com.song.cn.spark.scala.nx.arch.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:29
 * @Description:
 **/
object RDD_10_countByKey {

    def main(args: Array[String]): Unit = {

        // 初始化编程入口
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local")
        sparkConf.setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)

        val data = List((3, "Gnu"), (3, "Yak"), (5, "Mouse"), (3, "Dog"))
        val rdd = sc.parallelize(data, 2)

        /**
         * 统计每个key出现的次数
         */
        val result: collection.Map[Int, Long] = rdd.countByKey()

        for (a <- result) {
            println(a._1, a._2)
        }
    }
}
