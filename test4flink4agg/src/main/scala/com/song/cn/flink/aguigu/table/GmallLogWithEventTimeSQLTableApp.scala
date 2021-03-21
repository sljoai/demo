package com.song.cn.flink.aguigu.table

import com.alibaba.fastjson.JSON
import com.song.cn.flink.aguigu.sink.{MyKafkaUtil, StartUpLog}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}

import java.text.SimpleDateFormat

/**
 *  通过table sql 进行操作
 *  每10秒 统计一次各个渠道的个数 table sql 解决
 */
object GmallLogWithEventTimeSQLTableApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置时间特性为 EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


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

    // 指定 event time 字段
    val startupLogWithEventTimeDStream : DataStream[StartUpLog] = startUpLogDStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StartUpLog](Time.seconds(0L)) {
      override def extractTimestamp(element: StartUpLog): Long = {
        element.ts
      }
    })

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    // 将数据流转换成 table
    val startUpLogTable = tableEnv.fromDataStream(startupLogWithEventTimeDStream,
      // 必须指定
      // TODO：为啥这么复杂？不太方便把
      'mid,'uid,'appid,'area,'os,'ch,'logType,'vs,'logDate,'logHour,'logHourMinute,'ts.rowtime)


    val chCountTable: Table = tableEnv.sqlQuery("select ch,count(ch) from " + startUpLogTable +
      " group by ch, Tumble(ts, interval '10' SECOND)")

    // 经过聚合的结果，需要使用 toRetractStream
    val resultDStream: DataStream[(Boolean, (String, Long))] = chCountTable.toRetractStream[(String, Long)]
    resultDStream.filter(_._1).print()

    env.execute("Table LogApp Stream")

  }

}
