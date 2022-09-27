package com.atguigu.gmall.realtime.app


import java.{lang, util}
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{DauInfo, PageLog}
import com.atguigu.gmall.realtime.util.{MyBeanUtils, MyESUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date
import scala.collection.mutable.ListBuffer

object DwDDauApp {
    def main(args: Array[String]): Unit = {
        //0.还原状态
        revertDauState()

        //1.环境
        val sparkconf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
        val ssc = new StreamingContext(sparkconf, Seconds(5))

        val topic = "DWD_PAGE_LOG"
        val groupId = "dwd_dau_group"

        //2.读取偏移量
        val offsets: Map[TopicPartition, Long] = MyOffsetUtils.getOffset(topic, groupId)

        //3.接收Kafka数据
        var kafkaDStream: DStream[ConsumerRecord[String, String]] = null
        if (offsets != null && offsets.nonEmpty) {
            kafkaDStream = MyKafkaUtils.getKafkaDStream(topic, ssc, offsets, groupId)
        } else {
            kafkaDStream = MyKafkaUtils.getKafkaDStream(topic, ssc, groupId)
        }

        //4. 提取偏移量结束点
        var offsetRanges: Array[OffsetRange] = null
        kafkaDStream = kafkaDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        //5. 转化结构
        val pageLogDStream: DStream[PageLog] = kafkaDStream.map(
            record => {
                val jsonStr: String = record.value()
                val pageLog: PageLog = JSON.parseObject(jsonStr, classOf[PageLog])
                pageLog
            }
        )
        //pageLogDStream.print(100)


        //6.筛选去重
        // 6.1 自我审查 凡是有last_page_id，说明不是本次会话的第一个页面，直接过滤掉
        val filterDStreamByLastPage: DStream[PageLog] = pageLogDStream.filter(
            pageLog => {
                pageLog.last_page_id == null
            }
        )
        // 6.2 第三方审查 所有会话的第一个页面， 去redis中检查是否是今天的第一次
        val pageLogFilterDStream: DStream[PageLog] = filterDStreamByLastPage.mapPartitions(
            pageLogIter => {
                //获取redis连接
                val jedis: Jedis = MyRedisUtils.getJedisFromPool
                val filterList: ListBuffer[PageLog] = ListBuffer[PageLog]()
                val pageLogList: List[PageLog] = pageLogIter.toList
                println("过滤前 : " + pageLogList.size)
                for (pageLog <- pageLogList) {
                    val sdf = new SimpleDateFormat("yyyy-MM-dd")
                    val dateStr: String = sdf.format(new Date(pageLog.ts))
                    val dauKey: String = s"DAU:$dateStr"
                    val ifNew: lang.Long = jedis.sadd(dauKey, pageLog.mid)
                    //设置过期时间
                    jedis.expire(dauKey, 3600 * 24)
                    if (ifNew == 1L) {
                        filterList.append(pageLog)
                    }
                }
                jedis.close()
                println("过滤后: " + filterList.size)
                filterList.toIterator
            }
        )
        //pageLogFilterDStream.print(10)


        //7. 维度合并
        val dauInfoDStream: DStream[DauInfo] = pageLogFilterDStream.mapPartitions(
            pageLogIter => {
                val jedis: Jedis = MyRedisUtils.getJedisFromPool
                val dauInfoList: ListBuffer[DauInfo] = ListBuffer[DauInfo]()

                for (pageLog <- pageLogIter) {
                    //用户信息关联
                    val dimUserKey = s"DIM:USER_INFO:${pageLog.user_id}"
                    val userInfoJson: String = jedis.get(dimUserKey)
                    val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
                    //提取生日
                    val birthday: String = userInfoJsonObj.getString("birthday")
                    //提取性别
                    val gender: String = userInfoJsonObj.getString("gender")
                    //生日处理为年龄
                    var age: String = null
                    if (birthday != null) {
                        //闰年无误差
                        val birthdayDate: LocalDate = LocalDate.parse(birthday)
                        val nowDate: LocalDate = LocalDate.now()
                        val period: Period = Period.between(birthdayDate, nowDate)
                        val years: Int = period.getYears
                        age = years.toString
                    }

                    val dauInfo = new DauInfo()
                    //将PageLog的字段信息拷贝到DauInfo中
                    MyBeanUtils.copyProperties(pageLog, dauInfo)
                    dauInfo.user_gender = gender
                    dauInfo.user_age = age

                    //TODO 地区维度关联
                    val provinceKey: String = s"DIM:BASE_PROVINCE:${pageLog.province_id}"
                    val provinceJson: String = jedis.get(provinceKey)
                    if (provinceJson != null && provinceJson.nonEmpty) {
                        val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
                        dauInfo.province_name = provinceJsonObj.getString("name")
                        dauInfo.province_area_code = provinceJsonObj.getString("area_code")
                        dauInfo.province_3166_2 = provinceJsonObj.getString("iso_3166_2")
                        dauInfo.province_iso_code = provinceJsonObj.getString("iso_code")
                    }

                    //日期补充
                    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
                    val dtDate = new Date(dauInfo.ts)
                    val dtHr: String = dateFormat.format(dtDate)
                    val dtHrArr: Array[String] = dtHr.split(" ")
                    dauInfo.dt = dtHrArr(0)
                    dauInfo.hr = dtHrArr(1)

                    dauInfoList.append(dauInfo)

                }
                jedis.close()
                dauInfoList.toIterator
            }
        )
        //dauInfoDStream.print(100)


        // TODO 8.写入ES
        dauInfoDStream.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    dauInfoIter => {
                        // 因为是日活宽表，一天之内mid是不应该重复的
                        //转换数据结构，保证幂等写入
                        val dauInfos: List[(String, DauInfo)] = dauInfoIter.toList.map(dauInfo => (dauInfo.mid, dauInfo))
                        if (dauInfos.size > 0) {
                            //从数据中获取日期，拼接ES的索引名
                            val dauInfoT: (String, DauInfo) = dauInfos(0)
                            val dt = dauInfoT._2.dt
                            MyESUtils.bulkSaveIdempotent(dauInfos, s"gmall_dau_info_$dt")
                        }
                    }
                )
                // TODO 9.提交偏移量
                MyOffsetUtils.saveOffset(topic, groupId, offsetRanges)
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }

    /**
     * 状态还原
     *
     * 在每次启动实时任务时， 进行一次状态还原。 以ES为准, 将所有的mid提取出来，覆盖到Redis中.
     */

    def revertDauState(): Unit = {
        //从ES中查询到所有的mid
        val date: LocalDate = LocalDate.now()
        val indexName: String = s"gmall_dau_info_1018_$date"
        val fieldName: String = "mid"
        val mids: List[String] = MyESUtils.searchField(indexName, fieldName)
        //删除redis中记录的状态（所有的mid）
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        val redisDauKey: String = s"DAU:$date"
        jedis.del(redisDauKey)
        //将从ES中查询到的mid覆盖到Redis中
        if (mids != null && mids.size > 0) {
            /*for (mid <- mids) {
              jedis.sadd(redisDauKey , mid )
            }*/
            val pipeline: Pipeline = jedis.pipelined()
            for (mid <- mids) {
                pipeline.sadd(redisDauKey, mid) //不会直接到redis执行
            }

            pipeline.sync() // 到redis执行
        }

        jedis.close()
    }
}
