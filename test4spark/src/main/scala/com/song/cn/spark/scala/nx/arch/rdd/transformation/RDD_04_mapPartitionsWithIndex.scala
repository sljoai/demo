package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:41
 * @Description: 类似于mapPartitions，但func带有一个整数参数表示分片的索引值，因此在类型为T的RDD上运行时，
 *               func的函数类型必须是(Int, Interator[T]) => Iterator[U]
 **/
object RDD_04_mapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    val data = List((2, "cat"), (6, "mouse"), (7, "cup"), (3, "book"), (4, "tv"), (1, "screen"), (5, "heater"))
    val randRDD = sc.parallelize(data, 3)

    /**
     * 自己定义一个函数实现
     */
    def myfunc(index: Int, iter: Iterator[(Int, String)]): Iterator[String] = {
      iter.map(x => "[partID:" + index + ", val: " + x + "]")
    }

    val resultRDD: RDD[String] = randRDD.mapPartitionsWithIndex(myfunc)
    resultRDD.foreach(x => println(x))

    println("---------------------------------------")
    /**
     * 直接编写匿名函数
     */
    val resultRDD1: RDD[(Int, Int, String)] = randRDD.mapPartitionsWithIndex((index: Int, iter: Iterator[(Int,
      String)]) => {
      iter.map(x => (index, x._1, x._2))
    })
    resultRDD1.foreach(println)
  }

  def mapPartitionsWithIndex(sc: SparkContext): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8)
    sc.parallelize(list, 2).mapPartitionsWithIndex((index, iterator) => {
      val listBuffer: ListBuffer[String] = new ListBuffer
      while (iterator.hasNext) {
        listBuffer.append(index + "_" + iterator.next())
      }
      listBuffer.iterator
    }, true)
      .foreach(println(_))
  }
}
