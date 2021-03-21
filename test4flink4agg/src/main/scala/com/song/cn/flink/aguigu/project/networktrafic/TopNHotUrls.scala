package com.song.cn.flink.aguigu.project.networktrafic

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

/**
 * 自定义 Process Function
 */
class TopNHotUrls(topSize:Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{

  /**
   * 直接定义状态变量：懒加载，提高性能
   */
  lazy val  urlState:ListState[UrlViewCount] = getRuntimeContext.getListState(
    new ListStateDescriptor[UrlViewCount]("urlState",
    classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    // 把每条数据保存到状态中
    urlState.add(value)
    // 注册一个定时器，windowEnd + 10 时触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 10*1000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 从状态中获取所有的Url访问量
    val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for(urlView <- urlState.get()){
      allUrlViews += urlView
    }

    // 提前清空状态中的数据，释放空间
    urlState.clear()
    // 按照访问量从大到小排序
    val sortedUrls: ListBuffer[UrlViewCount] = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 将排名数据格式化，便于打印
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n\n")

    // TODO：为啥减去1呢？
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedUrls.indices) {
      val currentUrl:UrlViewCount = sortedUrls(i)
      // No1 商品ID= 浏览量=
      result.append("No").append(i+1).append(":")
        .append(" Url=").append(currentUrl.url)
        .append(" 流量=").append(currentUrl.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时变动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}
