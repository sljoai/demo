package com.song.cn.spark.scala.basic.func

object MethodAndFunction {

  /**
   * 定义一个方法：要求参数是一个函数，且函数的参数是两个Int类型
   * @param f
   * @return 返回值是Int类型
   */
  def m1(f:(Int,Int) => Int): Int = f(2,6)

  /**
   * 定义一个需要两个Int类型参数的方法
   * @param x
   * @param y
   * @return
   */
  def m2(x:Int,y:Int): Int = x+y

  /**
   * 定义一个计算数据不被写死的方法
   * @param f
   * @param x
   * @param y
   * @return
   */
  def m3(f:(Int,Int) => Int,x:Int,y:Int): Int = f(x,y)

  // 定义一个函数，参数是两个Int类型，返回值是一个Int类型
  val f1 = (x:Int,y:Int) => x+y

  val f2 = (m:Int,n:Int) => m*n

  // 定义一个传入函数的函数
  val f3 = (f:(Int,Int) => Int,x:Int,y:Int) => f(x,y)

  def main(args: Array[String]): Unit = {

    // 8
    println(m1(f1))

    // 12
    println(m1(f2))

    // 6
    println(m3(f1,2,4))

    // 6
    println(m3(m2,2,4))

    // 6
    println(f3(f1,2,4))

  }

}
