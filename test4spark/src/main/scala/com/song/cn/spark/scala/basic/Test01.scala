package com.song.cn.spark.scala.basic

object Test01 {
  def main(args: Array[String]): Unit = {
    val f1 = sayHello _
  }

  def sayHello(): Unit ={
    println("say hello~")
  }

}
