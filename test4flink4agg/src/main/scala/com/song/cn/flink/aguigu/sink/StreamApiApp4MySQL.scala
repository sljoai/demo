package com.song.cn.flink.aguigu.sink

import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.text.SimpleDateFormat

/**
 * 在Kibana上的Dev Tools中执行查询： GET gmall_20210316_flink/_search
 */
object StreamApiApp4MySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getKafkaSource("GMALL_STARTUP")
    val kafkaStream = env.addSource(kafkaConsumer)
    // kafkaStream.print()
    // 求各个渠道的销售量
    // 转换
    val startUpLogDStream = kafkaStream
      .map(jsonStr => {
          val startUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])
          // 时间转换
          val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
          val date = simpleDateFormat.format(startUpLog.ts).toString.split(" ")
          startUpLog.logDate =date(0)
          startUpLog.logHourMinute = date(1)
          startUpLog.logHour = date(1).split(":")(0)
          startUpLog
      })

    val myJdbcSink = new MyJdbcSink("insert into z_startup values(?,?,?,?)")
    val startUpForJdbcStream = startUpLogDStream.map(startUpLog =>
      Array(startUpLog.mid, startUpLog.ch, startUpLog.vs, startUpLog.uid))
    startUpForJdbcStream.addSink(myJdbcSink)

    env.execute("Kafka Consumer")
  }

}
