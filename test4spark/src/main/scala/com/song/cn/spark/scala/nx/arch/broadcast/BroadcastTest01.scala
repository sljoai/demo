package com.song.cn.spark.scala.nx.arch.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 作者： 马中华   https://blog.csdn.net/zhongqi2513
 * 时间： 2018/7/20 15:30
 *
 * 描述：
 */
object BroadcastTest01 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.set("spark.app.name", "rddtest")
    val sc = new SparkContext(sparkConf)

    // 定义广播变量
    val bigLong: Int = 10000
    // 这个 bcLong 广播变量会被序列化到每个worker节点，而不是像普通机制一样，给每个task发送一份
    val bcLong: Broadcast[Int] = sc.broadcast(bigLong)

    // 定义累加器
    val sumAccumulator = sc.longAccumulator("sum")

    val arrayRDD: RDD[Int] = sc.makeRDD(1 to 100, 10)

    arrayRDD.map(x => {
      // 使用累加器进行累加
      sumAccumulator.add(x)

      // 获取广播变量
      val bcValue = bcLong.value
      bcValue + x
    }).foreach(println)

    // 获取结果
    println(sumAccumulator.value)

    sc.stop()
  }
}
