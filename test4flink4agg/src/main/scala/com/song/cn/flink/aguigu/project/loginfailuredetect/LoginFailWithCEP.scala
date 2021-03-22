package com.song.cn.flink.aguigu.project.loginfailuredetect

import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCEP {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val loginStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.evnetTime * 1000)
      .keyBy(_.userId)


    // 定义模式匹配规则
    val loginFailPattern = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2L))

    // 在数据流中匹配出定义好的模式
    val patternStream = CEP.pattern(loginStream, loginFailPattern)

    // .select方法传入一个 pattern select function，当检测到定义好的模式序列时就会调用
    //TODO: 为啥不能使用 pattern:Map[String,Iterable[LoginEvent]] 来表示Pattern?
    //import scala.collection.Map
    patternStream.select(
      //(pattern:Map[String,Iterable[LoginEvent]]) =>{
      pattern =>{
      val first = pattern.getOrElse("begin", null).iterator.next()
      val second = pattern.getOrElse("next",null).iterator.next()
      (second.userId,first.ip,second.ip,first.eventType)
    }).print()

    env.execute("Login Failed Detection App.")


  }
}
