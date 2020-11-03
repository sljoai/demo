package com.song.cn.spark.scala.basic

import scala.io.StdIn

/**
 * 输入 样例
 */
object InputDemo {

  def main(args: Array[String]): Unit = {
    println("name")
    var name = StdIn.readLine()
    println("age")
    var age = StdIn.readInt()
    println("sal")
    var sal = StdIn.readDouble();
  }

}
