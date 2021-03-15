package com.song.cn.flink.aguigu.first

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 流式任务 WordCount 测试用例
 */
object DataStreamWcApp {

  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    // nc -lk 9999
    // mac 上无法安装，最终选择在linux上安装的: yum install -y nc
    val dataStream = environment.socketTextStream("192.168.80.110", 9999)
    import org.apache.flink.api.scala._
    dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)

    dataStream.print()

    environment.execute("World Count!")
  }
}
