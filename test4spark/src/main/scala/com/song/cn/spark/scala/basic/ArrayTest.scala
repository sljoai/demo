package com.song.cn.spark.scala.basic

import scala.collection.{JavaConverters, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions

object ArrayTest {

  def main(args: Array[String]): Unit = {

    // 多维数组
/*    val array1 = Array.ofDim[Int](3, 4)
    array1(1)(1) = 9
    for (item <- array1) {
      for (item2 <- item) {
        print(item2 + "\t")
      }
      println()
    }

    println("===================")

    for (i <- 0 to array1.length - 1) {
      for (j <- 0 to array1(i).length - 1) {
        printf("arr[%d][%d]=%d\t", i, j, array1(i)(j))
      }
      println()
    }*/

    // Scala数组和Java集合互相转换

    // Scala数组转Java的List
    var arr = ArrayBuffer("1","2","3")

    // TODO: 此种办法有问题，跑不通
/*    import scala.collection.JavaConversions.bufferAsJavaList
    var javaArr = new ProcessBuilder(arr)
    var arrList = javaArr.command()*/

    // Scala 转 Java
    val javaList: java.util.List[String] = JavaConversions.bufferAsJavaList(arr)

    // Java 转 Scala
    val scalaBuffer: mutable.Buffer[String] = JavaConversions.asScalaBuffer(javaList)
    for( elem <- scalaBuffer){
      println(elem)
    }

  }

}
