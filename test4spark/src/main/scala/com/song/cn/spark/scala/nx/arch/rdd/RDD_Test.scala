package com.song.cn.spark.scala.nx.arch.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 10:46
 * @Description: TODO
 **/
object RDD_Test {

  def main(args: Array[String]): Unit = {

    // 初始化编程入口
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("RDD_Test")
    val sc = new SparkContext(sparkConf)

    // 打印RDD
    //        resultRDD.foreach(println)


    // 这一段话就相当于   jj3.join(jj4)
    //        val coGroupResult: RDD[(String, (Iterable[Int], Iterable[String]))] = jj3.cogroup(jj4)
    //                       RDD[(String, (Int, String))]
    // 正常的 比如 map  filter等等这些算子都是在executor中执行
    // sparkContext是被初始化放在了driver中，
    // sparkContext对象和  foreach map  filter的这些算子的执行地不在同一个节点。
    // 算子作用在每个partition上。
    //
    //    val lastResult = coGroupResult.foreach(x => {
    ////          val value1 = x._2._1.mkString("-")
    ////          val value2 = x._2._2.mkString("-")
    ////      val value1 = sc.makeRDD(x._2._1.mkString("-").split("-"))
    ////      val value2 = sc.makeRDD(x._2._2.mkString("-").split("-"))
    ////      val tuples: RDD[(String, String)] = value1.cartesian(value2)
    //       val value1 = x._2._1.toArray
    //      val value2 = x._2._2.toArray
    //      //  （4，liutao1）  (4, liutao2)  (6, liutao1)   (6, liutao2)
    //      val tuples = new ListBuffer[(Int, String)]()
    //      for (aa <- value1){
    //        for (bb <- value2) {
    //          tuples += ((aa, bb))
    //        }
    //      }
    //      tuples.foreach( xx => {
    //          println(x._1, xx._1, xx._2)
    //      })
    //    })

    //  jj3.cartesian(jj4)

    //        rdd4.saveAsSequenceFile("file:///c:/sfout/")

    //    val mapvalues: RDD[(String, String)] = rdd4.mapValues((x:Int) => "hello" + x)
    //    mapvalues.foreach(t => println(t._1, t._2))

    //    val keys = rdd4.values
    //    for(t <- keys){
    //      println(t)
    //    }
    //    val stringToLong: collection.Map[String, Long] = rdd4.countByKey()
    //    for(t <- stringToLong){
    //      println(t._1 , t._2)
    //    }
    //    val tupleToLong: collection.Map[(String, Int), Long] = rdd4.countByValue()
    //    for(t <- tupleToLong){
    //      println(t._1._1, t._1._2, t._2)
    //    }

    // 按照
    //    val tuples: Array[(String, Int)] = jj1.top(3)
    //    tuples.foreach(x => println(x._1,  x._2))
    //    println("---------------------------------")
    //    val tuples2: Array[(String, Int)] =jj1.takeOrdered(3)
    //    tuples2.foreach(x => println(x._1,  x._2))

    //    val value111: RDD[(String, Int)] = rdd4.reduceByKey(_+_)
    //    val value222: collection.Map[String, Int] = rdd4.reduceByKeyLocally(_+_)
    //    Map("a" -> 1, "b" -> 2)
    //    Map(("a",1) , ("b", 2))
    //    for (t <- value222){
    //      println(t._1, t._2)
    //    }

    //    rdd1.subtract(rdd2).foreach(println)

    //    jj1.subtractByKey(jj2).foreach(println)

    //    val result1: RDD[Int] = jj2.coalesce(5, true).mapPartitionsWithIndex((index, iter) => {
    //      val ab = new ArrayBuffer[Int]()
    //      ab += index
    //      ab.toIterator
    //    })
    //    result1.foreach(println)

    //    val joinResult: RDD[(String, (Int, String))] = jj1.join(jj2)
    //    joinResult.foreach(x => println(x._1,  x._2._1,    x._2._2))

    //   （“hello”,   (1,1,1,1,1,1)）
    //    rdd4.groupByKey().foreach( x => print(x._1,  x._2.mkString("-")))
    //    rdd4.sortByKey().foreach(x => println(x._1, x._2))
    //    rdd4.sortBy( t => t._2, false).foreach(x => println(x._1, x._2))
    //    rdd4.map( t => t.swap).sortByKey().map(t => t.swap).foreach(x => println(x._1, x._2))
    //    rdd4.aggregateByKey(0)(_+_, _+_).foreach(x => println(x._1, x._2))

    //    println(rdd1.union(rdd2).count())
    //    rdd1.intersection(rdd2).foreach(println)
    //    rdd1.subtract(rdd2).foreach(println)
    //    rdd1.union(rdd2).distinct().foreach(println)

    //  每个元素被采样的概率  总采样率  0.5
    //    rdd1.sample(true, 0.5).foreach(println)  // 5  6  8

    //    rdd1.mapPartitionsWithIndex( (index, iter) => {
    //            val values = new ArrayBuffer[(Int, Int)]()
    //            var result: Int = 0
    //            while (iter.hasNext) {
    //              result += iter.next()
    //            }
    //            values += ((index, result))
    //            values.toIterator
    //    }).foreach(x => println(x._1, x._2))

    //    val tuple: (Int, Int) = rdd1.mapPartitions(iter => {
    //      val values = new ArrayBuffer[(Int, Int)]()
    //      var result: Int = 0
    //      var number: Int = 0
    //      while (iter.hasNext) {
    //        result += iter.next()
    //        number += 1
    //      }
    //      values += ((result, number))
    //      values.toIterator
    //    }).reduce((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2))
    //    println(tuple._1 / tuple._2)

    //    rdd3.map(_.split(" ")).foreach( x => println(x.toBuffer))
    //    rdd3.flatMap(_.split(" ")).foreach( x => println(x))
    //    println(rdd3.flatMap(_.split(" ")).collect().toBuffer)

    //    rdd1.map(_+1).foreach(println)

    //    rdd1.filter( _ % 2 == 1).foreach(println)

    //    val i: Int = rdd1.reduce((x:Int, y:Int) => x + y ); println(i)

    //    rdd1.distinct.foreach(println)

    //      println(rdd1.first())
    //      println(rdd1.take(3).mkString(","))

  }
}
