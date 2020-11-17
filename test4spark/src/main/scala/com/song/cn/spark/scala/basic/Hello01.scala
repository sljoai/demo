package com.song.cn.spark.scala.basic

object Hello01 {
  def main(args: Array[String]): Unit = {
    println("桃子="+peach(1))

    var f2 = f1
    var f3 = f1 _
    println(f2)
    println(f3) // <function0>
    println(f3())
  }

  def f1(): Int={
    100
  }

  /**
   * 计算桃子的个数
   * @param n
   * @return
   */
  def peach(n: Int): Int =
    if (n == 10) {
      return 1
    } else {
      return (peach(n+1)+1)*2
    }

}
