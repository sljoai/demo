package com.song.cn.spark.scala.basic.func

object Function03 {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4)

    //写法1: 因为map是一个高阶函数，因此也可以直接传入一个匿名函数，完成map
    println(list.map((x:Int)=>x + 1))

    //写法2: 当遍历list时，参数类型是可以推断出来的，如list=(1,2,3) list.map()
    // map中函数参数类型是可以推断的x的类型的，因此可以省略数据类型Int
    println(list.map((x)=>x + 1))

    //写法3: 当传入的函数，只有单个参数时，可以省去括号
    println(list.map(x=>x + 1))
    //写法4: 如果变量只在=>右边只出现一次，可以用_来代替
    println(list.map(_ + 1))

    println(list.reduce(_+_))
  }
}
