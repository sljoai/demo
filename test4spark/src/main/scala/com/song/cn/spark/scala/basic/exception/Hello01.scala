package com.song.cn.spark.scala.basic.exception

object Hello01 {
  def main(args: Array[String]): Unit = {
    try {
      //val r = 10 / 0
      f11()
    } catch {
      case ex: ArithmeticException=> println("捕获了除数为零的算数异常")
      case ex: NumberFormatException => println("捕获非数字类型的转换异常")
      case exception: Exception => println("捕获了异常")
    } finally {
      // 最终要执行的代码
      println("scala finally...")
    }


  }
  //等同于NumberFormatException.class
  @throws(classOf[NumberFormatException])
  def f11()  = {
    "abc".toInt
  }

}
