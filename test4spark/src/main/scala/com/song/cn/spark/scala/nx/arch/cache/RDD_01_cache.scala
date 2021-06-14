package com.song.cn.spark.scala.nx.arch.cache

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:48
 * @Description: TODO
 **/
object RDD_01_cache {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("RDD_CacheTest").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)


    val rdd = sparkContext.makeRDD(1 to 10)
    /**
     * 不缓存
     */
    val nocacheRDD = rdd.map(_.toString + "[" + System.currentTimeMillis + "]")
    println(nocacheRDD.collect.mkString(","))
    println(nocacheRDD.collect.mkString(","))

    println("-----------------------------------")

    /**
     * 有缓存
     */
    val cacheRDD = rdd.map(_.toString + "[" + System.currentTimeMillis + "]")
    cacheRDD.cache() // 在第一执行得到cacheRDD的数据的时候。 cache()方法就会把这个cacheRDD 放到内存
    println(cacheRDD.collect.mkString(",")) // 第一次触发执行，所以map操作执行了，得到了cacheRDD
    println(cacheRDD.collect.mkString(",")) // 第二次action触发执行的时候，就直接从内存中，获取cacheRDD的值

    cacheRDD.persist(StorageLevel.DISK_ONLY_2)

    cacheRDD.unpersist()
  }
}
