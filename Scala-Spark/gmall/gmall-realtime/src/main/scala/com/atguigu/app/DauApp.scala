package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handle.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

import java.text.SimpleDateFormat
import java.util.Date

object DauApp {
    def main(args: Array[String]): Unit = {
        //1.创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

        //2.创建StreamingContext
        val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

        //3.连接Kafka
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

        //测试数据是否正确读取
        /*kafkaDStream.foreachRDD(rdd => {
          rdd.foreach(record => {
            println(record.value())
          })
        })*/

        //4.将json数据转换为样例类，方便解析
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")

        val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partitions => {
            partitions.map(record => {
                //将json转换为样例类
                val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
                //补充字段分别为 logdate loghour
                //yyyy-MM-dd HH
                val str: String = sdf.format(new Date(startUpLog.ts))
                startUpLog.logDate = str.split(" ")(0)
                startUpLog.logHour = str.split(" ")(1)

                startUpLog
            })
        })
        startUpLogDStream.count().print()

        //进行批次间去重
        val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)
        filterByRedisDStream.count().print()

        //进行批次内去重
        val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterbyGroup(filterByRedisDStream)
        filterByGroupDStream.count().print()

        //将去重后的数据保存至redis，为下一批数据去重用
        DauHandler.saveMidToRedis(filterByRedisDStream)


        //将数据保存到HBase中
        filterByGroupDStream.foreachRDD(rdd => {
            rdd.saveToPhoenix(
                "GMALL2021_DAU",
                Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
                HBaseConfiguration.create,
                Some("hadoop102,hadoop103,hadoop104:2181")
            )
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
