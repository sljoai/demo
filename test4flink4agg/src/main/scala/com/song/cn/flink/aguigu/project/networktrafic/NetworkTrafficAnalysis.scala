package com.song.cn.flink.aguigu.project.networktrafic

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

import java.text.SimpleDateFormat

object NetworkTrafficAnalysis {
  def main(args: Array[String]): Unit = {

    // 创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显示设置Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.readTextFile("/Users/sljoai/Workspace/Java/demo/test4flink4agg/src/main/resources/apachetest.log")
      .map(line => {
        val lineArray = line.split(" ")
        val simpleDataFormat = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
        val timeStamp = simpleDataFormat.parse(lineArray(3)).getTime()
        ApacheLogEvent(lineArray(0),lineArray(1),timeStamp,lineArray(5),lineArray(6))
      })
      // 特殊情况：给定的样例数据timestamp是升序的
      // 指定时间戳和watermark
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = {
          element.eventTime
        }
      })
      // 过滤分流
      .filter(_.method == "GET")

      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      // 获取每个商品在每个窗口的点击量的数据流
      .aggregate(new CountAgg(), new WindowResultFunction())
      // 按窗口分组
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(3))
      .print()

    env.execute("Network Traffic Analysis Job")
  }

}

/**
 * 输入数据格式
 * @param ip
 * @param userId
 * @param eventTime
 * @param method
 * @param url
 */
case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)

case class UrlViewCount(url:String,windowEnd:Long,count:Long)