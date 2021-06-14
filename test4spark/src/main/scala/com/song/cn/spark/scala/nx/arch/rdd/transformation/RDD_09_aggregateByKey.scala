package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:46
 * @Description: TODO
 **/
object RDD_09_aggregateByKey {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    val data = List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2))
    val pairRDD = sc.parallelize(data, 2)

    def myfunc(index: Int, iter: Iterator[(String, Int)]): Iterator[String] = {
      iter.map(x => "[partID:" + index + ", val: " + x + "]")
    }

    val resultRDD = pairRDD.mapPartitionsWithIndex(myfunc).collect
    resultRDD.foreach(println)

    /**
     * 第一次测试
     * 先按分区聚合再总的聚合，每次要跟初始值交流  例如：aggregateByKey(0)(_+_,_+_) 对K/V的RDD进行操作
     */
    val resultRDD2 = pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect
    resultRDD2.foreach(println)

    /**
     * 第二次测试：
     * 先按分区聚合再总的聚合，每次要跟初始值交流  例如：aggregateByKey(100)(_+_,_+_) 对K/V的RDD进行操作
     */
    val resultRDD3 = pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect
    resultRDD3.foreach(println)
  }

  def aggregateByKey(sc: SparkContext): Unit = {
    val list = List("you,jump", "i,jump")
    sc.parallelize(list)
      .flatMap(_.split(","))
      .map((_, 1))
      .aggregateByKey(0)(_ + _, _ + _)
      .foreach(tuple => println(tuple._1 + "->" + tuple._2))
  }
}
