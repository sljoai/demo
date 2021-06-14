package com.song.cn.spark.scala.nx.arch.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 12:28
 * @Description:
 **/
object RDD_07_takeSample {

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
    val sampleArray: Array[Int] = rdd.takeSample(true, 10)

    sampleArray.foreach(println)
    println("随机抽取的元素的个数：" + sampleArray.count(x => true))
  }
}
