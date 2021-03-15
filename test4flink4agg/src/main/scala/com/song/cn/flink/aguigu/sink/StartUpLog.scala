package com.song.cn.flink.aguigu.sink

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

import scala.beans.BeanProperty

case class StartUpLog(@BeanProperty mid: String,
                      @BeanProperty uid: String,
                      @BeanProperty appid: String,
                      @BeanProperty area: String,
                      @BeanProperty os: String,
                      @BeanProperty ch: String,
                      @BeanProperty logType: String,
                      @BeanProperty vs: String,
                      @BeanProperty var logDate: String,
                      @BeanProperty var logHour: String,
                      @BeanProperty var logHourMinute: String,
                      @BeanProperty var ts: Long
                ) {
//  override def toString: String = JSON.toJSONString(this,SerializerFeature.PrettyFormat)
}
