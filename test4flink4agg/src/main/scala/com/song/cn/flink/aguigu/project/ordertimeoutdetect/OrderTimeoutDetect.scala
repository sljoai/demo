package com.song.cn.flink.aguigu.project.ordertimeoutdetect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeoutDetect {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    )).assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    // 定义pattern
    val orderPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.orderType == "create")
      //TODO: followedBy 与 next 区别是什么呢？
      .followedBy("follow")
      .where(_.orderType == "pay")
      .within(Time.minutes(15L))

    // 定义一个输出标签，用于标明侧输出流
    val orderTimeoutTag = OutputTag[OrderResult]("orderTimeout")

    // 匹配定义好的模式，得到一个pattern Stream
    val orderPatternStream = CEP.pattern(orderEventStream, orderPattern)

    import scala.collection.Map
    // 从 pattern stream 中获取输出流
    val completeResult = orderPatternStream.select(orderTimeoutTag) (
      // 对于超时部分的序列，会调用 pattern timeout function
      (pattern: Map[String, Iterable[OrderEvent]], timeStamp: Long) => {
        val timeoutOderId = pattern.getOrElse("begin", null).iterator.next().orderId
        OrderResult(timeoutOderId, "timeout")
      }
    ) (
      // 对于正常匹配的部分，就会调用 pattern select
      (pattern: Map[String, Iterable[OrderEvent]]) => {
        val payedOrderId = pattern.getOrElse("follow", null).iterator.next().orderId
        OrderResult(payedOrderId, "success")
      }
    )

    // 拿到输出超时的结果
    val timeoutResult = completeResult.getSideOutput(orderTimeoutTag)

    // 打印正常输出的序列
    completeResult.print()
    // 打印超时部分的序列
    timeoutResult.print()

    env.execute("Order Timeout Detect App")


  }

}
