package com.song.cn.flink.aguigu.sink

import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object MyRedisUtil {
  val conf = new FlinkJedisPoolConfig.Builder()
    .setHost("hadoop01")
    .setPort(6379)
    .build()

  def getRedisSink(): RedisSink[(String,Int)] ={
    new RedisSink[(String,Int)](conf,new MyRedisMapper)
  }

  class MyRedisMapper extends RedisMapper[(String,Int)]{
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "channel_count")
      // new RedisCommandDescription(RedisCommand.SET  )
    }

    /**
     * 获取 value
     * @param t
     * @return
     */
    override def getValueFromData(t: (String, Int)): String = t._2.toString

    /**
     * 获取 key
     * @param t
     * @return
     */
    override def getKeyFromData(t: (String, Int)): String = t._1
  }

}
