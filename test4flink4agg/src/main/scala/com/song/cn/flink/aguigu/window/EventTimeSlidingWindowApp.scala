package com.song.cn.flink.aguigu.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeSlidingWindowApp {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.设置时间特性
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

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
          // 指定以第二个字段作为 EventTime
          t._2
        }
      })
      // 设置并行度为1，方便测试
      .setParallelism(1)

    // 窗口必须是步长的整数倍
    val countWindow = textWithEventTimeDStream.keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.milliseconds(4000L),Time.milliseconds(2000L)))
      .sum(2)
    countWindow.print("SlidingWindow...")

    // 记录
    countWindow.map(str => (str._1,str._2,str._2.toString,str._3))
      .keyBy(0)
      .reduce((str1,str2) => (str2._1,str1._2,str1._3.concat(":").concat(str2._3),str1._4+str2._4))
      .print("ConcatTS...")
    // TODO: 输出结果有点迷惑，执行图是怎样的呢？
    //text...:7> abc 3000
    //text...:8> abc 5000
    //SlidingWindow...:8> (abc,3000,1)
    //ConcatTS...:8> (abc,3000,3000,1)
    //text...:1> abc 2900
    //text...:2> abc 6000
    //text...:3> abc 9999
    //SlidingWindow...:8> (abc,3000,3)
    //SlidingWindow...:8> (abc,5000,2)
    //ConcatTS...:8> (abc,3000,3000:3000,4)
    //ConcatTS...:8> (abc,3000,3000:3000:5000,6)
    //text...:4> abc 1000
    //text...:5> abc 10000
    //text...:6> abc 11000
    //SlidingWindow...:8> (abc,6000,2)
    //ConcatTS...:8> (abc,3000,3000:3000:5000:6000,8)
    //text...:7> abc 150000
    //SlidingWindow...:8> (abc,9999,3)
    //SlidingWindow...:8> (abc,10000,2)
    //ConcatTS...:8> (abc,3000,3000:3000:5000:6000:9999,11)
    //ConcatTS...:8> (abc,3000,3000:3000:5000:6000:9999:10000,13)
    //text...:8> abc 12000
    //text...:1> abc 13000
    //text...:2> abc 15000
    //text...:3> abc 16000
    //text...:4> abc 160000
    //SlidingWindow...:8> (abc,150000,1)
    //SlidingWindow...:8> (abc,150000,1)
    //ConcatTS...:8> (abc,3000,3000:3000:5000:6000:9999:10000:150000,14)
    //ConcatTS...:8> (abc,3000,3000:3000:5000:6000:9999:10000:150000:150000,15)

    // TODO: 如何记录是哪些时间被划分到当前窗口呢？

    streamEnv.execute("Window And Watermark Test.")

  }

}
