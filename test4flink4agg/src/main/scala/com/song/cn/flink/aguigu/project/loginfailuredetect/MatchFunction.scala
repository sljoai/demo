package com.song.cn.flink.aguigu.project.loginfailuredetect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class MatchFunction extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {
  /**
   * 直接定义状态变量：懒加载，提高性能
   */
  lazy private val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("LoginFailState",classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, out: Collector[LoginEvent]): Unit = {
    loginState.add(value)
    // 注册定时器
    ctx.timerService().registerEventTimeTimer(value.evnetTime + 1)
  }

  /**
   * 实现 onTimer 触发操作
   *
   * @param timestamp
   * @param ctx
   * @param out
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
    val allLogins: ListBuffer[LoginEvent] = ListBuffer()
    import scala.collection.JavaConversions._
    for (login <- loginState.get()) {
      allLogins += login
    }
    // 清空State
    loginState.clear()
    //
    if (allLogins.length > 1) {
      out.collect(allLogins.head)
    }
  }
}
