package com.song.cn.spark.scala.nx.arch.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:50
 * @Description: TODO
 **/
object RDD_02_partitionBy {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Spark_Partition_Test")
    val sc = new SparkContext(sparkConf)

    //模拟5个分区的数据
    val data = sc.parallelize(1 to 10, 5)

    //根据尾号转变为10个分区，分写到10个文件
    val indexRDD: RDD[Int] = data.map((_, 1)).partitionBy(new MyPartitioner(3))
      .mapPartitionsWithIndex((index, iter) => {
        val listBuffer = new ListBuffer[Int]()
        listBuffer += index
        println(iter.toList.mkString(","))
        listBuffer.toIterator
      })

    indexRDD.foreach(println)
  }
}

//自定义分区类，需继承Partitioner类
class MyPartitioner(numParts: Int) extends Partitioner {
  //覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    key.toString.toInt % numPartitions
  }

  //覆盖分区数
  override def numPartitions: Int = numParts
}
