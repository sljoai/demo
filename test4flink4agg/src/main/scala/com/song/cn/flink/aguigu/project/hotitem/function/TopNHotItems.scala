package com.song.cn.flink.aguigu.project.hotitem.function

import com.song.cn.flink.aguigu.project.hotitem.bean.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

/**
 * 自定义 Process Function<br>
 * 获取某个窗口中前N名的热门点击商品，key为窗口时间戳，输出TopN的结果字符串
 *
 * @param topSize
 */
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  private var itemState: ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 命名状态变量的名字和类型
    val itemStateDescriptor = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
    itemState = getRuntimeContext().getListState(itemStateDescriptor)
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 将数据保存到状态中
    itemState.add(value)
    // 注册定时器，触发时间定位windowEnd +1，触发时说明window已经收集完成所有数据
    // 当程序看到windowEnd+1的水位线watermark时，触发onTimer回调函数
    // TODO: 不理解这里的含义？！
    ctx.timerService.registerEventTimeTimer(value.windowEnd + 1)
  }

  /**
   *
   * @param timestamp
   * @param ctx
   * @param out
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 获取所有的商品点击信息
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    // Java 与 Scala 类型转换
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }
    // 清除状态中的数据，释放空间
    itemState.clear()

    // 按照点击量从大到小排序
    // 函数柯里化
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)


    // 将排名数据格式化，便于打印输出
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n\n")

    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedItems.indices) {
      val currentItem:ItemViewCount = sortedItems(i)
      // No1 商品ID= 浏览量=
      result.append("No").append(i+1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时变动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}
