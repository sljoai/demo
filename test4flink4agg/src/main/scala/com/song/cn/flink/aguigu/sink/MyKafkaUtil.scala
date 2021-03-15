package com.song.cn.flink.aguigu.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

import java.util.Properties

object MyKafkaUtil {

  val properties = new Properties()

  properties.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
  properties.put("group.id", "gmall")

  def getKafkaSource(topic: String): FlinkKafkaConsumer011[String] = {
    val kafkaConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
    kafkaConsumer
  }

  def getKafkaSink(topic: String): FlinkKafkaProducer011[String] = {
    val flinkKafkaProducer = new FlinkKafkaProducer011[String](properties.getProperty("bootstrap.servers"),
      topic, new SimpleStringSchema())
    flinkKafkaProducer
  }

}
