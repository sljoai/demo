package com.song.cn.flink.aguigu.project.hotitem.bean

/**
 * 输入数据样例类
 *
 * @param userId     用户ID
 * @param itemId    商品ID
 * @param categoryId 类别ID
 * @param behavior   用户行为描述
 * @param timeStamp  操作时间戳
 */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timeStamp: Long)
