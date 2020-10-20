package com.song.cn.spark.scala.streaming

import com.alibaba.fastjson.JSON

object JsonTest {
  def main(args: Array[String]): Unit = {
    case class Orders1(order_id: String,
                       user_id: String,
                       eval_set: String,
                       order_number: String,
                       order_dow: String,
                       hour: String,
                       day: String)


    val s = """{"user_id": 795, "hour": 19, "order_id": 2322381, "eval_set": "prior", "order_dow": 2, "order_number": 3, "day": 16.0}"""
    val mess = JSON.parseObject(s, classOf[Orders])
    println(mess.order_id, mess.order_dow, mess.hour)
  }

}
