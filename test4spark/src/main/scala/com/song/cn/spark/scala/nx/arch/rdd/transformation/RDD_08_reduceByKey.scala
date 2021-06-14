package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 10:46
 * @Description: TODO
 **/
object RDD_08_reduceByKey {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    /**
     * key-value 类型的测试
     * 在一个(K,V)对的数据集上使用，返回一个(K,V)对的数据集，key相同的值，都被使用指定的reduce函数聚合到一起。
     * 和groupByKey类似，任务的个数是可以通过第二个可选参数来配置的。
     */

    val data = List(("a", 1), ("a", 3), ("b", 2), ("b", 1))
    val rdd3 = sc.parallelize(data)

    val resultRDD = rdd3.reduceByKey((x, y) => x + y)

    val ff1 = (x: String, y: String) => x + y
    val ff2 = (x: Int, y: Int) => x + y
    rdd3.reduceByKey(ff2)

    // 打印结果
    resultRDD.foreach(println)
  }

  def reduceByKey(sc: SparkContext): Unit = {
    val list = List(("武当", 99), ("少林", 97), ("武当", 89), ("少林", 77))
    val mapRDD = sc.parallelize(list)
    val resultRDD = mapRDD.reduceByKey(_ + _)
    resultRDD.foreach(tuple => println("门派: " + tuple._1 + "->" + tuple._2))
  }
}
