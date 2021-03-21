package com.song.cn.flink.aguigu.project.hotitem.function

import com.song.cn.flink.aguigu.project.hotitem.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction


/**
 * 自定义实现聚合函数： 统计窗口中的条数，减少state的存储压力
 * 每出现一条记录累加一次
 * IN: 需要聚合的对象类别
 * ACC：聚合器的中间结果类别
 * OUT: 聚合器输出的结果类别
 */
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
