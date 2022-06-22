package com.atguigu.bean


/**
 * 用来存放预警日志数据
 * @param mid
 * @param uids
 * @param itemIds
 * @param events
 * @param ts
 */
case class CouponAlertInfo(mid: String,
                           uids: java.util.HashSet[String],
                           itemIds: java.util.HashSet[String],
                           events: java.util.List[String],
                           ts: Long)

