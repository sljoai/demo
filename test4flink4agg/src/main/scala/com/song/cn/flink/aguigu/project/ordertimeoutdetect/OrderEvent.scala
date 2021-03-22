package com.song.cn.flink.aguigu.project.ordertimeoutdetect

/**
 * 输入数据样例类的数据流
 */
case class OrderEvent(orderId:Long, orderType:String,eventTime:Long)
