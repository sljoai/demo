package com.song.cn.spark.scala.basic.func

object FunctionCurry02 {
  def main(args: Array[String]): Unit = {
    //常规方法
    def checkEq(s1: String, s2: String) = {
      //1.先将其全部转成大写(或小写)
      val s11 = s1.toLowerCase
      val s22 = s2.toLowerCase
      //2.比较是否相等
      s11.equals(s22)
    }

    val s1 = "aaBcn"
    val s2 = "aabcN"

    println(checkEq(s1, s2))

    //使用柯里化，可以化的要求是: 接受多个参数的函数都可以转化为接受单个参数的函数

    //柯里化这个题的思路
    //1. 给String的功能进行扩展(隐式类)
    //2. 编写一个checkEq( ss : String )( f: (String, String)=> Boolean )的方法
    //3. 比如: s.checkEq(ss: String)//接收一个参数，完成将s 和 ss转成大写/或小写的
    //4. f: (String, String)=> Boolean 函数完成比较功能

    val str1 = "aaBcn"
    val str2 = "aabcN"
    //调用方式1
    println(str1.checkEq(str2)(eq)) //在checkEq内部调用eq返回true or false
    println(str1.checkEq(str2)((x: String, y: String) => x.equals(y))) //直接写匿名函数作为第二个参数值
    //上面的简化写法
    println(str1.checkEq(str2)((x, y) => x.equals(y)))
    //继续简化【因为x,y在 => 右边只出现过一次，因此可以去掉括号，使用 _】
    println("-----------------------------")
    println(str1.checkEq(str2)(_.equals(_)))
  }

  def eq(s1: String, s2: String): Boolean = {
    s1.equals(s2)
  }

  implicit class TestEq(s: String) {
    def checkEq(ss: String)(f: (String, String) => Boolean): Boolean = {
      // 把字符串变成小写
      f(s.toLowerCase, ss.toLowerCase)
    }
  }


}
