package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.util.control.Breaks.{break, breakable}


/**
 * 总结：
 *   1.滑动窗口
 *   2.根据预警的指标信息（要写入ES的指标信息）创建预警样例类
 *   3.根据mid进行分组，统计每个mid的预警指标，并封装进预警样例类
 *   4.根据统计的预警指标判断是否达到预警标准，并创建二元组用一位布尔值标记来记录是否达到预警标准，另一位预警样例类
 *   5.过滤出达到预警标准的数据
 */
object AlertApp {
    def main(args: Array[String]): Unit = {
        //创建SparkConf
        val sparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

        //创建StreamingContext
        val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

        //读取Kafka数据创建DStream流数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

        //将DStream流的JSON数据类型转换为样例类数据类型EventLog   再转为元组 (mid, EventLog)
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val midToEventLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
            partition.map(record => {
                val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
                val times: String = sdf.format(new Date(eventLog.ts))

                //补全时间字段
                eventLog.logDate = times.split(" ")(0)
                eventLog.logHour = times.split(" ")(1)

                (eventLog.mid, eventLog)
            })
        })
        //测试
        //midToEventLogDStream.print()


        //对DStream流进行开滑动窗口（5分钟5秒钟）
        val windowMidToLogDStream: DStream[(String, EventLog)] = midToEventLogDStream.window(Minutes(5))
        //测试
        //windowMidToLogDStream.print()


        //分组聚合(mid)
        val midToIterLogDStream: DStream[(String, Iterable[EventLog])] = windowMidToLogDStream.groupByKey()



        //根据条件筛选数据
        /**
         * 三次及以上用不同账号登录并领取优惠劵，（过滤出领取优惠券行为，把相应的uid存放到set集合中，并统计个数）
         * 并且过程中没有浏览商品。（如果用户有浏览商品的行为，则不作分析）
         */
        //返回疑似预警日志
        //对组内的数据进行分析处理后创建预警日志样例类 –CouponAlertInfo，并对预警日志数据进行判定是否触发预警条件，通过true或false标志，最后组合成元组数据类型进行返回 (boolean, CouponAlertInfo)
        val boolToCouponAlertDStream: DStream[(Boolean, CouponAlertInfo)] = midToIterLogDStream.mapPartitions(partition => {
            partition.map { case (mid, iter) => {
                //创建用来存放领优惠券的用户id
                val uids: util.HashSet[String] = new util.HashSet[String]()
                //创建用来存放领优惠券所涉及的商品id
                val itemIds = new util.HashSet[String]()
                //创建用来存放用户所涉及事件的集合
                val events: util.ArrayList[String] = new util.ArrayList[String]()

                //创建一个标志位，用来判断用户是否有浏览商品行为
                var bool: Boolean = true

                breakable {
                    iter.foreach(log => {
                        //添加用户涉及的行为
                        events.add(log.evid)
                        if ("clickItem".equals(log.evid)) {
                            //浏览商品行为
                            bool = false
                            break()
                        } else if ("coupon".equals(log.evid)) {
                            //添加用户id
                            uids.add(log.uid)
                            //添加涉及到的商品id
                            itemIds.add(log.itemid)
                        }
                    })
                }

                (uids.size() >= 3 & bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
            }
            }
        })


        //生成预警日志
        val couponAlertInfoDStream: DStream[CouponAlertInfo] = boolToCouponAlertDStream.filter(_._1).map(_._2)
        //测试
        couponAlertInfoDStream.print()


        //将预警日志写入ES并去重（将mid和分钟组合起来 作为es中id 可以达到去重的作用）
        couponAlertInfoDStream.foreachRDD(rdd => {
            rdd.foreachPartition(partition => {
                val list: List[(String, CouponAlertInfo)] = partition.toList.map(log => {
                    (log.mid + log.ts / 1000 / 60, log)
                })
                MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEX + "0625", list)
            })
        })

        //启动任务并阻塞
        ssc.start()
        ssc.awaitTermination()
    }
}







