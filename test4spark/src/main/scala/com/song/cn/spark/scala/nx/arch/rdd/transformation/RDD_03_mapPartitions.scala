package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:40
 * @Description: 类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，
 *               func的函数类型必须是Iterator[T] => Iterator[U]
 **/
object RDD_03_mapPartitions {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    val data = List((2, "cat"), (6, "mouse"), (7, "cup"), (3, "book"), (4, "tv"), (1, "screen"), (5, "heater"))
    val randRDD = sc.parallelize(data, 3)

    /**
     * 直接编写匿名函数
     */
    randRDD.mapPartitions(iter => {
      iter.map(x => x._1 + "," + x._2)
    }).foreach(println)
  }

  def mapParations(sc: SparkContext): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD = sc.parallelize(list, 2)

    listRDD.mapPartitions(iterator => {
      val newList: ListBuffer[Int] = ListBuffer()
      while (iterator.hasNext) {
        newList.append(iterator.next())
      }
      println(newList.max, "---------- ")
      newList.iterator
    }).foreach(name => println(name))
  }
}
