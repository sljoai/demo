package com.song.cn.spark.scala.basic.func

object MyTest1 {
  def main(args: Array[String]): Unit = {
    // 输入字符串和Double值，变成整数求和
    // 完整写法
    val f2:(String,Double) => Int=(a: String,b: Double) => a.toInt + b.toInt
    // 简化版1，后面的函数写法省略全部类型
    val f3:(String,Double) => Int=(a,b) => a.toInt + b.toInt
    // 简化版2，省略函数签名
    val f4 = (a:String,b:Double) => a.toInt + b.toInt
    var a = "123"
    var b = 456.99d
    println(s"a: $a, b: $b, f2: {}, f3: {}, f4: {}.",f2(a,b),f3(a,b),f4(a,b))

    // 高阶函数
    def test1(x: Double) = {
      (y: Double) => x*x*y
    }
    val res = test1(2.0)(3.0)
    println("res = " +res)

  }
}
