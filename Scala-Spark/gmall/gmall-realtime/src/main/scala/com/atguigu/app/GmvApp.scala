package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object GmvApp {
    def main(args: Array[String]): Unit = {
        //1.创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("GMV").setMaster("local[*]")

        //2.创建StreamingContext
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

        //3.读取Kafka数据，创建DStream
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

        //4.将DStream流的JSON数据类型转换为样例类数据类型，并补全时间字段
        val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.mapPartitions(partition => {
            partition.map(record => {
                val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
                orderInfo.create_date = orderInfo.create_time.split(" ")(0)
                orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
                orderInfo
            })
        })
        //测试
        //orderInfoDStream.print()

        //5.将DStream流的数据写到Phoenix
        orderInfoDStream.foreachRDD(rdd => {
            rdd.saveToPhoenix(
               "GMALL2021_ORDER_INFO",
                Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
                HBaseConfiguration.create(),
                Some("hadoop102,hadoop103,hadoop104:2181")
            )
        })


        //6.启动任务并阻塞
        ssc.start()
        ssc.awaitTermination()
    }
}
