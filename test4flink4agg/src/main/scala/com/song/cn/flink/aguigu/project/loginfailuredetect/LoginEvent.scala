package com.song.cn.flink.aguigu.project.loginfailuredetect

/**
 * 定义输入的登陆事件流
 * @param userId
 * @param ip
 * @param eventType
 * @param evnetTime
 */
case class LoginEvent(userId: Long,ip:String,eventType:String,evnetTime:Long)
