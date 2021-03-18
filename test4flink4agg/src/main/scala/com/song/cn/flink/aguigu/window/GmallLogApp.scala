package com.song.cn.flink.aguigu.window

import com.alibaba.fastjson.JSON
import com.song.cn.flink.aguigu.sink.{MyKafkaUtil, StartUpLog}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.text.SimpleDateFormat

object GmallLogApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getKafkaSource("GMALL_STARTUP")
    val kafkaStream = env.addSource(kafkaConsumer)
    // kafkaStream.print()
    // 求各个渠道的销售量
    val startUpLogDStream = kafkaStream
      .map(jsonStr => {
        val startUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])
        // 时间转换
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        val date = simpleDateFormat.format(startUpLog.ts).toString.split(" ")
        startUpLog.logDate = date(0)
        startUpLog.logHourMinute = date(1)
        startUpLog.logHour = date(1).split(":")(0)
        startUpLog
      })
    // 统计个数
    val chKeyedStream = startUpLogDStream
      .map(startUpLog => (startUpLog.ch, 1))
      //.keyBy(_._1)
      .keyBy(0)
    // 滚动窗口
//    val tumblingWindow = chKeyedStream.timeWindow(Time.seconds(10L)).sum(1)
//    tumblingWindow.print()
    // 滑动窗口
//    val slidingWindow = chKeyedStream.timeWindow(Time.seconds(10L), Time.seconds(2L)).sum(1)
//    slidingWindow.print()

    // 计数窗口
//    val countWindow = chKeyedStream.countWindow(10L).sum(1)
//    countWindow.print()

    env.execute("Kafka Consumer")
  }

}
