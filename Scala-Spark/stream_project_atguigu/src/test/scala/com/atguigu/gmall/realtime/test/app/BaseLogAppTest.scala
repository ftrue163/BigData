package com.atguigu.gmall.realtime.test.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.test.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.atguigu.gmall.realtime.test.util.{MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 消费分流
 * 1. 接收Kafka数据流
 *
 * 2. 转换数据结构:
 * 通用的数据结构: Map 或者 JsonObject
 * 专用的数据结构: bean
 *
 * 3. 分流 : 将数据拆分到不同的主题中
 * 启动主题: DWD_START_LOG
 * 页面访问主题: DWD_PAGE_LOG
 * 页面动作主题:DWD_PAGE_ACTION
 * 页面曝光主题:DWD_PAGE_DISPLAY
 * 错误主题:DWD_ERROR_INFO
 */
object BaseLogAppTest {
    def main(args: Array[String]): Unit = {

        //创建配置对象
        val sparkConf: SparkConf = new SparkConf().setAppName("base_log_app").setMaster("local[4]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        //原始主题
        //val ods_base_topic: String = "ODS_BASE_LOG"
        val ods_base_topic: String = "ODS_BASE_LOG_1018"
        //启动主题
        val dwd_start_log: String = "DWD_START_LOG"
        //页面访问主题
        val dwd_page_log: String = "DWD_PAGE_LOG"
        //页面动作主题
        val dwd_page_action: String = "DWD_PAGE_ACTION"
        //页面曝光主题
        val dwd_page_display: String = "DWD_PAGE_DISPLAY"
        //错误主题
        val dwd_error_info: String = "DWD_ERROR_INFO"

        //消费组
        val group_id: String = "ods_base_log_group"


        //1. 接收Kafka数据流
        /*val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtils.getKafkaDStream(ods_base_topic, ssc, group_id)*/


        //补充:  ========= 通过redis手动保存偏移量，实现精准一次性 =====================
        // 从redis中读取偏移量
        val offsets: Map[TopicPartition, Long] = MyOffsetUtils.getOffset(ods_base_topic, group_id)

        // 判断是否能读取到
        var kafkaDStream: DStream[ConsumerRecord[String, String]] = null
        if (offsets != null && offsets.nonEmpty) {
            //redis中有记录offset
            //1. 接收Kafka数据流
            kafkaDStream =
                MyKafkaUtils.getKafkaDStream(ods_base_topic, ssc, offsets, group_id)
        } else {
            //redis中没有记录offset
            //1. 接收Kafka数据流
            kafkaDStream =
                MyKafkaUtils.getKafkaDStream(ods_base_topic, ssc, group_id)
        }


        //在数据转换前, 提取本次流中offset的结束点，
        var offsetRanges: Array[OffsetRange] = null // driver
        kafkaDStream = kafkaDStream.transform( // 每批次执行一次
            rdd => {
                println(rdd.getClass.getName)
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        //kafkaDStream.map(_.value()).print(3)

        //2. 转换数据结构
        val jsonDStream: DStream[JSONObject] = kafkaDStream.map(
            record => {

                val value: String = record.value()
                println(value)
                val jsonObject: JSONObject = JSON.parseObject(value)

                jsonObject
            }
        )

        //3.切分数据分流
        jsonDStream.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    iter => {
                        for (jsonObj <- iter) {
                            //TODO 错误日志
                            //分流错误日志 判断是否是错误日志，如果是错误日志，直接发送
                            val errorObj: JSONObject = jsonObj.getJSONObject("err")
                            if (errorObj != null) {
                                MyKafkaUtils.send(dwd_error_info, jsonObj.toJSONString)
                            } else {
                                //提取公共信息
                                val commonObj: JSONObject = jsonObj.getJSONObject("common")
                                val mid: String = commonObj.getString("mid")
                                val uid: String = commonObj.getString("uid")
                                val ar: String = commonObj.getString("ar")
                                val ch: String = commonObj.getString("ch")
                                val os: String = commonObj.getString("os")
                                val md: String = commonObj.getString("md")
                                val vc: String = commonObj.getString("vc")
                                val isNew: String = commonObj.getString("is_new")
                                val ts: Long = commonObj.getLong("ts")

                                //分流页面日志
                                val pageObj: JSONObject = jsonObj.getJSONObject("page")
                                if (pageObj != null) {
                                    //提取字段
                                    val pageId: String = pageObj.getString("page_id")
                                    val pageItem: String = pageObj.getString("item")
                                    val pageItemType: String = pageObj.getString("item_type")
                                    val lastPageId: String = pageObj.getString("last_page_id")
                                    val duringTime: Long = pageObj.getLong("during_time")

                                    //封装bean
                                    val pageLog =
                                        PageLog(mid, uid, ar, ch, isNew, md, os, vc, pageId, lastPageId, pageItem, pageItemType, duringTime, ts)
                                    //发送kafka
                                    MyKafkaUtils.send(dwd_page_log, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                                    //分流动作日志
                                    val actionArrayObj: JSONArray = jsonObj.getJSONArray("actions")
                                    if (actionArrayObj != null && actionArrayObj.size() > 0) {
                                        for (i <- 0 until actionArrayObj.size()) {
                                            val actionObj: JSONObject = actionArrayObj.getJSONObject(i)
                                            val actionId: String = actionObj.getString("action_id")
                                            val actionItem: String = actionObj.getString("item")
                                            val actionItemType: String = actionObj.getString("item_type")
                                            //TODO actionts
                                            val actionTs: Long = actionObj.getLong("ts")

                                            //封装Bean
                                            //val pageActionLog = PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, pageId, lastPageId, pageItem, pageItemType, duringTime, actionId, actionItem, actionItemType, actionTs, ts)

                                            //发送Kafka
                                            //MyKafkaUtils.send(dwd_page_action, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))

                                        }
                                    }

                                    //分流曝光日志
                                    val displayArrayObj: JSONArray = jsonObj.getJSONArray("displays")
                                    if (displayArrayObj != null && displayArrayObj.size() > 0) {
                                        for (i <- 0 until displayArrayObj.size()) {
                                            val displayObj: JSONObject = displayArrayObj.getJSONObject(i)
                                            val displayType: String = displayObj.getString("display_type")
                                            val displayItem: String = displayObj.getString("item")
                                            val displayItemType: String = displayObj.getString("item_type")
                                            val displayOrder: String = displayObj.getString("order")
                                            val displayPosId: String = displayObj.getString("pos_id")

                                            //封装Bean
                                            val displayLog = PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, pageId, lastPageId, pageItem, pageItemType, duringTime, displayType, displayItem, displayItemType, displayOrder, displayPosId, ts)
                                            //发送Kafka
                                            MyKafkaUtils.send(dwd_page_display, JSON.toJSONString(displayLog, new SerializeConfig(true)))
                                        }
                                    }

                                }

                                //分流启动日志
                                val startObj: JSONObject = jsonObj.getJSONObject("start")
                                if (startObj != null) {
                                    val entry: String = startObj.getString("entry")
                                    val loadingTimeMs: Long = startObj.getLong("loading_time_ms")
                                    val openAdId: String = startObj.getString("open_ad_id")
                                    val openAdMs: Long = startObj.getLong("open_ad_ms")
                                    val openAdSkipMs: Long = startObj.getLong("open_ad_skip_ms")

                                    //封装Bean
                                    val startLog = StartLog(mid, uid, ar, ch, isNew, md, os, vc, entry, openAdId, loadingTimeMs, openAdMs, openAdSkipMs, ts)
                                    //发送Kafka
                                    MyKafkaUtils.send(dwd_start_log, JSON.toJSONString(startLog, new SerializeConfig(true)))
                                }

                            }
                        }

                        // 一个批次一个分区提交一次
                        MyKafkaUtils.flush()
                    }
                )
                //foreachRDD里边  driver中执行，一个批次执行一次
                //提交偏移量
                MyOffsetUtils.saveOffset(ods_base_topic, group_id, offsetRanges)
            }
        )


        ssc.start()
        ssc.awaitTermination()
    }
}
