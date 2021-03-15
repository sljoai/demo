package com.song.cn.flink.aguigu.sink

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.streaming.api.scala.{SplitStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 验证：
 * 1 ./redis-cli
 * 2. hgetall channel_count
 */
object StreamApiSinkRedisApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getKafkaSource("GMALL_STARTUP")
    val kafkaStream = env.addSource(kafkaConsumer)
    // kafkaStream.print()
    // 求各个渠道的销售量
    // 转换
    val startUpLogDStream = kafkaStream
      .map(jsonStr => JSON.parseObject(jsonStr, classOf[StartUpLog]))

    // 统计各渠道的个数
    val keyedStream = startUpLogDStream.map(startUpLog => (startUpLog.ch, 1))
      .keyBy(_._1)
      .reduce((ch1, ch2) => (ch1._1, ch1._2 + ch2._2))

    val redisSink = MyRedisUtil.getRedisSink()

    keyedStream.addSink(redisSink)


    env.execute("Kafka Consumer")
  }

}
