package com.song.cn.spark.scala.nx.arch.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:30
 * @Description:
 **/
object RDD_14_aggregate {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    /**
     * TODO 第一次测试
     */
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)

    def myfunc(index: Int, iter: Iterator[(Int)]): Iterator[String] = {
      iter.map(x => "[partID:" + index + ", val: " + x + "]")
    }

    val resultRDD = rdd1.mapPartitionsWithIndex(myfunc).collect
    resultRDD.foreach(println)

    /**
     * 第一个参数： 初始值
     * 第二个参数： partition内的聚合逻辑
     * 第三个参数： 所有分区的结果数据的聚合逻辑
     */
    val resultValue = rdd1.aggregate(0)(math.max(_, _), _ + _)
    println(resultValue)

    /**
     * TODO 第二次测试
     */
    val rdd2 = sc.parallelize(List("a", "b", "c", "d", "e", "f"), 2)

    def myfunc2(index: Int, iter: Iterator[(String)]): Iterator[String] = {
      iter.map(x => "[partID:" + index + ", val: " + x + "]")
    }

    val resultRDD2 = rdd2.mapPartitionsWithIndex(myfunc2).collect
    resultRDD2.foreach(println)

    // 注意区别
    val resultValue2: String = rdd2.aggregate("")(_ + _, _ + _)
    println(resultValue2)
    val resultValue22: String = rdd2.aggregate("x")(_ + _, _ + _)
    println(resultValue22)

    /**
     * TODO 第三次测试
     */
    val rdd3 = sc.parallelize(List("12", "23", "345", "4567"), 2)
    val resultValue3 = rdd3.aggregate("")((x, y) => math.max(x.length, y.length).toString,
      (x, y) => x + y)
    println(resultValue3)
    val resultValue33 = rdd3.aggregate("")((x, y) => math.min(x.length, y.length).toString,
      (x, y) => x + y)
    println(resultValue33)
    val rdd33 = sc.parallelize(List("12", "23", "345", ""), 2)
    val resultValue34 = rdd33.aggregate("")((x, y) => math.min(x.length, y.length).toString,
      (x, y) => x + y)
    println(resultValue34)
  }

  def aggregate(sc: SparkContext): Unit = {
    val textRDD = sc.parallelize(List("A", "B", "C", "D", "D", "E"), 3)
    val tuple: (Int, String) = textRDD.aggregate((0, ""))(
      (acc, value) => {
        (acc._1 + 1, acc._2 + ":" + value)
      },
      (acc1, acc2) => {
        (acc1._1 + acc2._1, acc1._2 + ":" + acc2._2)
      }
    )
    println(tuple._1, tuple._2)
  }

  def aggregate1(sc: SparkContext): Unit = {
    val numbers: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 22))

    val lastResult: (Int, Int) = numbers.aggregate((0, 0))(
      (acc, number) => (acc._1 + number, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )
    println(lastResult._1 * 1D / lastResult._2)
  }
}
