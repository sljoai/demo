package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 19:09
 * @Description:
 **/
object RDD_30_glom {

    def main(args: Array[String]): Unit = {

        // 初始化编程入口
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local")
        sparkConf.setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.parallelize(1 to 20, 3)

        rdd.glom().collect().foreach(x => {
            for (a <- x) print(a + "\t")
            println("")
        })
    }
}
