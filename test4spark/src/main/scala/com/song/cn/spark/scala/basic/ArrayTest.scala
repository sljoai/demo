package com.song.cn.spark.scala.basic

import scala.collection.mutable.ArrayBuffer

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

    import scala.collection.JavaConversions.bufferAsJavaList
    var javaArr = new ProcessBuilder(arr)
    var arrList = javaArr.command()

    println(arrList)



  }

}
