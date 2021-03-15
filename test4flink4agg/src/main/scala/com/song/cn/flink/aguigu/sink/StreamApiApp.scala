package com.song.cn.flink.aguigu.sink

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.text.SimpleDateFormat

object StreamApiApp {
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
        startUpLog.logDate =date(0)
        startUpLog.logHourMinute = date(1)
        startUpLog.logHour = date(1).split(":")(0)
        startUpLog
      })
    // 统计个数
    //    val chKeyedStream = startUpLogDStream
    //      .map(startUpLog => (startUpLog.ch, 1))
    //      .keyBy(_._1)
    //    val chSumDStream = chKeyedStream
    //      .reduce((ch1, ch2) => (ch1._1, ch1._2 + ch2._2))
    //
    //    chSumDStream.print()
    // 根据标签进行切分
    val splitStream: SplitStream[StartUpLog] = startUpLogDStream.split { startUpLog =>
      var flag: List[String] = null
      if (startUpLog.ch == "appstore") {
        flag = List("apple", "usa")
      } else if (startUpLog.ch == "huawei") {
        flag = List("android", "china")
      } else {
        flag = List("android", "other")
      }
      flag
    }

    val appleStream = splitStream.select("apple", "china")
    val otherStream = splitStream.select("other")

    //    appleStream.print("apple-china")
    //    otherStream.print("other")

    // 将两个流合并
//    val connStream: ConnectedStreams[StartUpLog, StartUpLog] = appleStream.connect(otherStream)
    //    val allDataStream: DataStream[String] = connStream.map(
    //      (startUpLog1: StartUpLog) => startUpLog1.ch,
    //      (startUpLog2: StartUpLog) => startUpLog2.ch
    //    )
    //    allDataStream.print("all")

    //    val unionDStream:DataStream[StartUpLog] = appleStream.union(otherStream)
    //    unionDStream.print("UNION")
    val kafkaProducer = MyKafkaUtil.getKafkaSink("apple_topic")
    // TODO: 输出有问题（只输出"{}"）
    // 给 StartUpLog 的 属性添加 @BeanProperty注解即可，参考：https://my.oschina.net/u/4396547/blog/3289157
    val value:DataStream[String] = appleStream.map(startupLog => JSON.toJSONString(startupLog, SerializerFeature.PrettyFormat))
    // TODO: 注意pom文件中 flink-streaming-scala 与 flink-connector-redis 的引入顺序, 是什么原因导致的呢？
    value.addSink(kafkaProducer)


    env.execute("Kafka Consumer")
  }

}
