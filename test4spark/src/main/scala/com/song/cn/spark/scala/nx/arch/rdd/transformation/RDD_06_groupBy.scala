package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 19:14
 * @Description:
 **/
object RDD_06_groupBy {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    /**
     * 分组
     */
    val rdd1 = sc.parallelize(1 to 9, 3)
    val resultRDD1: RDD[(String, Iterable[Int])] = rdd1.groupBy(x => {
      if (x % 2 == 0) "even" else "odd"
    })
    resultRDD1.foreach(println)
    println("--------------------------------------")

    /**
     * 分组
     */
    val rdd2 = sc.parallelize(1 to 9, 3)

    def myfunc(a: Int): Int = {
      a % 2
    }

    val resultRDD2: RDD[(Int, Iterable[Int])] = rdd2.groupBy(myfunc)
    resultRDD2.foreach(println)
    println("--------------------------------------")

    /**
     * 分组
     */
    val partitioner = new MyPartitioner()

    //        val rdd3 = sc.parallelize(1 to 9, 3)
    //        val rdd4 = rdd3.groupBy(x => x, partitioner)

    val rdd3 = sc.parallelize(List(("a", 11), ("b", 22), ("a", 33)), 3)
    val rdd4 = rdd3.groupBy(x => x._1, partitioner)

    rdd4.foreachPartition(x => {
      for (y <- x) {
        print(y)
      }
      println()
    })
  }

  class MyPartitioner extends Partitioner {

    override def getPartition(key: Any): Int = {
      key match {
        case null => 0
        case key: Int => key % numPartitions
        case _ => key.hashCode % numPartitions
      }
    }

    override def numPartitions: Int = 2

    override def equals(other: Any): Boolean = {
      other match {
        case h: MyPartitioner => true
        case _ => false
      }
    }
  }

}