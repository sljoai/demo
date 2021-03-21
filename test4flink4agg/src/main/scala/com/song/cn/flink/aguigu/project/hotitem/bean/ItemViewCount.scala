package com.song.cn.flink.aguigu.project.hotitem.bean

/**
 * 输出数据样例类 商品点击量（窗口操作输出类型）
 *
 * @param itemId    商品ID
 * @param windowEnd 窗口结束时间戳
 * @param count     点击量
 */
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
