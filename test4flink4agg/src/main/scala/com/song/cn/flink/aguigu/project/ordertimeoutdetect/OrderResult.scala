package com.song.cn.flink.aguigu.project.ordertimeoutdetect

/**
 * 输出数据样例类的数据流
 * @param orderId
 * @param eventType
 */
case class OrderResult(orderId:Long,eventType:String)
