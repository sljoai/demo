package com.song.cn.spark.scala.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * Created by zheng on 2018/1/19.
  */
object StreamFromKafka {

  case class Order(order_id: String,
                   user_id: String,
                   eval_set: String,
                   order_number: String,
                   order_dow: String,
                   hour: String,
                   day: String)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.print("Usage: Collect log from Kafka <groupid> <topic> <Execution_time>")
      System.exit(1)
    }

    val Array(group_id, topic, exectime, dt, path) = args

    //      zookeeper IP:Port
    val zkHostIp = Array("134", "135", "129").map("192.168.174." + _)
    val ZK_QUORUM = zkHostIp.map(_ + ":2181").mkString(",")

    //      val dt = getNowDate()
    val numThreads = 1

    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(exectime.toInt))
    val topicSet = topic.split(",").toSet
    val topicMap = topicSet.map((_, numThreads.toInt)).toMap

    //      通过Receiver接收kafka数据
    val mesR = KafkaUtils.createStream(ssc, ZK_QUORUM, group_id, topicMap).map(_._2)

    def rdd2DataFrame(rdd: RDD[String]): DataFrame = {
      val spark = SparkSession
        .builder()
        .appName("Streaming Form Kafka Static")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport()
        .getOrCreate()
      import spark.implicits._
      rdd.map { x =>
        val mess = JSON.parseObject(x, classOf[Orders])
        Order(mess.order_id,
          mess.user_id,
          mess.eval_set,
          mess.order_number,
          mess.order_dow,
          mess.hour,
          mess.day)
      }.toDF()
    }

    //uv 10s内的统计
    //      val log = mesR.map(x => JSON.parseObject(x, classOf[Orders]).user_id)
    //        .map((_,1L))
    //        .reduceByKeyAndWindow(_+_,_-_,Seconds(30))
    //      log.print()
    //      log.saveAsTextFiles(path)


    val log = mesR.foreachRDD { rdd =>
      val df = rdd2DataFrame(rdd)
      df.withColumn("dt", lit(dt))
        .write.mode(SaveMode.Append)
        .insertInto("badou.order_partition")
    }

    //      log.filter(_._1=="right").map(_._2)
    //        .foreachRDD{ rdd =>
    //          if(dynamic.toInt==1) rddSave(rdd,Schema.NewKeyWordSchema,"badou.fact_log_static")
    //          else rddSaveTable(rdd,Schema.NewKeyWordSchema,"badou.fact_log")
    //      }
    //      log.filter(_._1=="wrong").map(_._2)
    //        .foreachRDD{rdd=>
    //          val dt = getNowDate()
    //          val rdd1 = rdd.map(r=>Row(r.getString(0),r.getString(1),dt))
    //          if(dynamic.toInt==1) rddSave(rdd1,Schema.WrongNewKeyWordSchema,"badou.error_fact_log_static")
    //          else rddSaveTable(rdd1,Schema.WrongNewKeyWordSchema,"badou.error_fact_log")
    //      }

    ssc.start()
    ssc.awaitTermination()
  }

  def rddSave(rdd: RDD[Row], schema: StructType, tableName: String) {
    val records = rdd.coalesce(1)
    val spark = SparkSession
      .builder()
      .appName("Streaming Form Kafka Static")
      .enableHiveSupport()
      .getOrCreate()
    val df = spark.createDataFrame(records, schema)
    df.write.mode(SaveMode.Append).saveAsTable(tableName)
  }

  def rddSaveTable(rdd: RDD[Row], schema: StructType, tableName: String) {
    val records = rdd.coalesce(1)
    //        打开hive动态分区和非严格模式
    val spark = SparkSession.builder()
      //      .config("spark.sql.warehouse.dir","hdfs:///user/hive/warehouse/badou.db")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    //    spark.sql("set hive.exec.dynamic.partition=true;")
    //    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")
    val df = spark.createDataFrame(records, schema)
    df.write.mode(SaveMode.Append).insertInto(tableName)
  }


  def getNowDate() = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val today = dateFormat.format(now)
    today
  }
}
