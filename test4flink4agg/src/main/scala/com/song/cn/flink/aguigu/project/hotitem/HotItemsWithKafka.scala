
package com.song.cn.flink.aguigu.project.hotitem

import com.song.cn.flink.aguigu.project.hotitem.bean.UserBehavior
import com.song.cn.flink.aguigu.project.hotitem.function.{CountAgg, TopNHotItems, WindowResultFunction}
import com.song.cn.flink.aguigu.sink.MyKafkaUtil
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 使用Kafka作为输入源
 */
object HotItemsWithKafka {
  def main(args: Array[String]): Unit = {

    // 创建一个env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 显示设置Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 创建kafka
    env.addSource(MyKafkaUtil.getKafkaSource("hotitems"))
      .map(line => {
        val lineArray = line.split(",")
        UserBehavior(lineArray(0).toLong, lineArray(1).toLong, lineArray(2).toInt, lineArray(3), lineArray(4).toLong)
      })
      // 特殊情况：给定的样例数据timestamp是升序的
      // 指定时间戳和watermark
      .assignAscendingTimestamps(_.timeStamp * 1000)
      // 分流
      .filter(_.behavior == "pv")
      // TODO：注意：如果此处按照"_.itermId"填写的时，则需要aggregate中WindowFunction的第三个泛型(K)保持与Window Key一致
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      // 获取每个商品在每个窗口的点击量的数据流
      .aggregate(new CountAgg(), new WindowResultFunction())
      // 按窗口分组
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))
      .print()

    env.execute("Hot Items Job")
  }
}
