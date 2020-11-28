package com.song.cn.spark.scala.basic.`object` {

  package object `package` {
    var name = "sljoai"

    def sayOk(): Unit = {
      println("package object sayOk!")
    }
  }

  package `package` {

    class TestClass {
      def test(): Unit = {
        println(name) // 这里的name就是包对象package中声明的name
        sayOk() // 包对象package中声明的sayOk
      }
    }

    object Test {
      def main(args: Array[String]): Unit = {
        val t = new TestClass()
        t.test()
        println("name=" + name)
      }
    }

  }

}


