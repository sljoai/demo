package com.song.cn.flink.aguigu.first

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem

/**
 * 启动时添加环境变量
 * --inputPath /Users/sljoai/Workspace/Java/demo/test4flink4agg/input/word.txt
 * --outPath /Users/sljoai/Workspace/Java/demo/test4flink4agg/output/word_count.csv
 */
object DataSetWcApp {
  def main(args: Array[String]): Unit = {
    val parameter = ParameterTool.fromArgs(args)
    val inputPath = parameter.get("inputPath")
    val outPath = parameter.get("outPath")
    val environment = ExecutionEnvironment.getExecutionEnvironment

    val textDataSet = environment.readTextFile(inputPath)

    import org.apache.flink.api.scala._
    val aggSet = textDataSet.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    // 以csv的格式保存数据，且支持覆盖
    aggSet.writeAsCsv(outPath,"\n",",",FileSystem.WriteMode.OVERWRITE)
    // 设置算子并发度
    aggSet.setParallelism(1)

    environment.execute("DataSet Word Count!")
  }

}
