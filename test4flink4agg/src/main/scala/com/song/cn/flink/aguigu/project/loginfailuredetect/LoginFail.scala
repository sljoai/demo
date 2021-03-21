package com.song.cn.flink.aguigu.project.loginfailuredetect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object LoginFail {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.evnetTime *1000)
      //加入UserId进行分流
      .filter(_.eventType=="fail")
      .keyBy(_.userId)
      .process(new MatchFunction())
      .print()

    env.execute("Login Failed Detection App.")

  }
}
