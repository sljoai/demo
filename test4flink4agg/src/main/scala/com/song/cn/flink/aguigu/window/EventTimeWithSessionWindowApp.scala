package com.song.cn.flink.aguigu.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * text...:7> abc 1000
text...:8> abc 3000
text...:1> abc 4000
text...:2> abc 6000
text...:3> abc 9000
TumblingWindow...:8> (abc,1000,4)
text...:4> abc 12000
TumblingWindow...:8> (abc,9000,1)
text...:5> abc 13999
text...:6> abc 16000
text...:7> abc 19000
TumblingWindow...:8> (abc,12000,2)
TumblingWindow...:8> (abc,16000,1)
 */
object EventTimeWithSessionWindowApp {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.设置时间特性
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.streaming.api.scala._
    val socketStream = streamEnv.socketTextStream("hadoop01", 9999)

    socketStream.print("text...")
    // 2.获取数据源
    val countStream: DataStream[(String, Long, Int)] = socketStream.map(str => {
      val strs = str.split(" ")
      (strs(0), strs(1).toLong, 1)
    })

    // 3.指定水位和抽取eventTime的方法
    val textWithEventTimeDStream: DataStream[(String, Long, Int)] = countStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(1000L)) {
        override def extractTimestamp(t: (String, Long, Int)): Long = {
          t._2
        }
      })
      // 设置并行度为1，方便测试
      .setParallelism(1)

    // 窗口必须是步长的整数倍
    val countWindow = textWithEventTimeDStream.keyBy(0)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(2000L))).sum(2)
    countWindow.print("TumblingWindow...")

    streamEnv.execute("Window And Watermark Test.")

  }

}
