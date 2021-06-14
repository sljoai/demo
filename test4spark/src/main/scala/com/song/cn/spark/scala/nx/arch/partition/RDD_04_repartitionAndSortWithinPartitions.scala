package com.song.cn.spark.scala.nx.arch.partition

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:51
 * @Description:
 **/
object RDD_04_repartitionAndSortWithinPartitions {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    repartitionAndSortWithinPartitions(sc)
  }

  def repartitionAndSortWithinPartitions(sc: SparkContext): Unit = {
    val list = List(1, 4, 55, 66, 33, 48, 23)
    val listRDD = sc.parallelize(list, 1)
    listRDD.map(num => (num, num))
      .repartitionAndSortWithinPartitions(new HashPartitioner(2))
      .mapPartitionsWithIndex((index, iterator) => {
        val listBuffer: ListBuffer[String] = new ListBuffer
        while (iterator.hasNext) {
          listBuffer.append(index + "_" + iterator.next())
        }
        listBuffer.iterator
      }, false)
      .foreach(println(_))

  }
}
