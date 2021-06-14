package com.song.cn.spark.scala.nx.arch.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/18 11:48
 * @Description:
 *
 * 分成多个桶，然后计算每个桶中的频数
 * * 输入：参数 bucketCount 可以是一个数字，也可以是一个列表
 * * 输出：结果为一个元组，元组包含两个列表分别是桶（直方图的边界）和直方图的频数
 * * 注意：
 * 1、桶必须是排好序的，并且不包含重复元素，至少有两个元素
 * 2、所有直方图的结果集合区右边是开区间，最后一个区间除外。
 *
 * 参数buckets是一个数字的情况: 根据桶的总数来计算
 * 先用排好序的数组的边界值来得出两个桶之间的间距 (11 - 1)/5 = 2
 **/
object RDD_21_histogram {

    def main(args: Array[String]): Unit = {

        // 初始化编程入口
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local")
        sparkConf.setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)

        /**
         * 第一个案例
         */
        val dataRDD = sc.parallelize(List(1.0, 1.2, 1.3, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 9.0, 11), 3)
        val resultValue: (Array[Double], Array[Long]) = dataRDD.histogram(5)

        println(resultValue._1.length, resultValue._2.length)

        for (index <- 0 until resultValue._2.length) {
            println(resultValue._1(index) + "\t" + resultValue._2(index))
        }

        println("-------------------------------------------------")
        /**
         * 第二个案例
         */
        val dataRDD2 = sc.parallelize(List(9.1, 1.0, 1.2, 2.1, 1.3, 5.0, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 10.0, 8.9,
            5.5), 3)
        val resultValue1: (Array[Double], Array[Long]) = dataRDD2.histogram(6)

        for (index <- 0 until resultValue1._2.length) {
            println(resultValue1._1(index) + "\t" + resultValue1._2(index))
        }

    }
}
