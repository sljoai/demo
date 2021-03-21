package com.song.cn.flink.aguigu.table

import com.alibaba.fastjson.JSON
import com.song.cn.flink.aguigu.sink.{MyKafkaUtil, StartUpLog}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.scala.table2TableConversions
import org.apache.flink.table.api.{Table, TableEnvironment}

import java.text.SimpleDateFormat

/**
 * 使用 Table 接口 输出渠道（ch）属于 appstore 的日志信息
 */
object GmallLogTableApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getKafkaSource("GMALL_STARTUP")

    val kafkaStream = env.addSource(kafkaConsumer)

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

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val startUpLogTableStream = tableEnv.fromDataStream(startUpLogDStream)

    val filterTable: Table = startUpLogTableStream.select("mid,ch").filter("ch='appstore'")

    // 把Table转化成数据流
    val middleDStream = filterTable.toAppendStream[(String, String)]
    middleDStream.print()

    env.execute("Table LogApp Stream")

  }

}
