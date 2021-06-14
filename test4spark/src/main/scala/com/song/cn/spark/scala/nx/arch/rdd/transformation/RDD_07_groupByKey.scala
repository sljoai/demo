package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:44
 * @Description: TODO
 **/
object RDD_07_groupByKey {

    def main(args: Array[String]): Unit = {

        // 初始化编程入口
        val sparkConf = new SparkConf()
        sparkConf.setMaster("local")
        sparkConf.setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)

        /**
         * 第一波测试：
         * 在一个(K,V)的RDD上调用，返回一个(K, Iterator[V])的RDD
         */
        val rdd1 = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
        val rdd2: RDD[(Int, String)] = rdd1.keyBy(_.length)
        val resultRDD: RDD[(Int, Iterable[String])] = rdd2.groupByKey()
        resultRDD.foreach(x => {
            println(x._1)
            for (y <- x._2) {
                print(y + "\t")
            }
            println()
        })
    }

    def groupByKey(sc: SparkContext): Unit = {
        val list = List(("武当", "张三丰"), ("峨眉", "灭绝师太"), ("武当", "宋青书"), ("峨眉", "周芷若"))
        val listRDD = sc.parallelize(list)
        val groupByKeyRDD = listRDD.groupByKey()
        groupByKeyRDD.foreach(t => {
            val menpai = t._1
            val iterator = t._2.iterator
            var people = ""
            while (iterator.hasNext) people = people + iterator.next + " "
            println("门派:" + menpai + "人员:" + people)
        })
    }
}
