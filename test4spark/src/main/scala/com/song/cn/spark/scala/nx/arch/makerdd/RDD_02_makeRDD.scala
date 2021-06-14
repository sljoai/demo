package com.song.cn.spark.scala.nx.arch.makerdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/18 10:45
 * @Description: 从scala集合创建
 **/
object RDD_02_makeRDD {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    /**
     * 第一波
     */
    val array = Array("hello huangbo", "hello xuzheng", "hello wangbaoqiang")
    val kvArray = Array(("a", 1), ("a", 1), ("b", 2), ("c", 3), ("d", 4), ("c", 2), ("a", 2), ("f", 2))
    val rdd3: RDD[String] = sc.makeRDD(array, 4)
    val rdd4: RDD[(String, Int)] = sc.makeRDD(kvArray)

    /**
     * 第二波
     */
    val tArray1 = Array(("a", 6), ("b", 5), ("d", 3), ("c", 4), ("e", 2), ("f", 1))
    val tArray2 = Array(("a", "huangbo"), ("b", "xu"), ("c", "abc"), ("d", "liutao"))
    val rdd5: RDD[(String, Int)] = sc.makeRDD(tArray1)
    val rdd6: RDD[(String, String)] = sc.makeRDD(tArray2, 1)

    /**
     * 第三波
     */
    val tArray3 = Array(("a", 1), ("b", 2), ("c", 3), ("d", 4), ("c", 5), ("d", 6))
    val tArray4 = Array(("a", "bo"), ("b", "xu"), ("c", "shi"), ("d", "li"), ("c", "liu"), ("d", "ma"))
    val rdd7: RDD[(String, Int)] = sc.makeRDD(tArray3)
    val rdd8: RDD[(String, String)] = sc.makeRDD(tArray4)
  }
}
