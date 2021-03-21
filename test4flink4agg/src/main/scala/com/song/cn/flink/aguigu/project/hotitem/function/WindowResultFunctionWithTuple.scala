package com.song.cn.flink.aguigu.project.hotitem.function

import com.song.cn.flink.aguigu.project.hotitem.bean.ItemViewCount
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 自定义实现 WindowFunction，将每个key对应的窗口聚合后的结果带上其他信息进行输出
 * IN: 主键商品ID
 * OUT: ItemViewCount(主键商品ID(itemId),窗口(window.getEnd),点击量)
 * KEY: 主键数据类别
 */
class WindowResultFunctionWithTuple extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, window.getEnd, count))
  }
}
