package com.song.cn.spark.scala.nx.arch.rdd.transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:42
 * @Description: 根据fraction指定的比例对数据进行采样，
 *               可以选择是否使用随机数进行替换，seed用于指定随机数生成器种子
 *
 *
 **/
object RDD_20_sample {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    val data = 1 to 100
    val rdd = sc.parallelize(data, 3)

    /**
     * sample参数解释：
     * 1、withReplacement：元素可以多次抽样(在抽样时替换)
     * 2、fraction：期望样本的大小作为RDD大小的一部分，
     * 当withReplacement=false时：选择每个元素的概率;分数一定是[0,1] ；
     * 当withReplacement=true时：选择每个元素的期望次数; 分数必须大于等于0。
     * 3、seed：随机数生成器的种子
     */
    val sampleRDD: RDD[Int] = rdd.sample(true, 0.1)

    sampleRDD.foreach(println)
    println("随机抽取的元素的个数：" + sampleRDD.count())
  }

  def sample(sc: SparkContext): Unit = {
    val list = 1 to 100
    val listRDD = sc.parallelize(list)
    // 有无放回的抽样， 抽取样本的比例，随机种子
    listRDD.sample(false, 0.2, 1).foreach(num => print(num + " "))
  }
}
