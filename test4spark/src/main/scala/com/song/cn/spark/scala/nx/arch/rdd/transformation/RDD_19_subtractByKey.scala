package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:43
 * @Description: TODO
 **/
object RDD_19_subtractByKey {

  def main(args: Array[String]): Unit = {


    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    /**
     * substractByKey和基本转换操作中的subtract类似只不过这里是针对K的，
     * 返回在主RDD中出现，并且不在otherRDD中出现的元素
     */
    val data1 = List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2))
    val pairRDD1 = sc.parallelize(data1, 2)

    val data2 = List(("cat", 2), ("pig", 5), ("rat", 4), ("cat", 12), ("dog", 12), ("rat", 2))
    val pairRDD2 = sc.parallelize(data2, 2)

    val resultRDD: RDD[(String, Int)] = pairRDD1.subtractByKey(pairRDD2)

    resultRDD.foreach(println)
  }
}
