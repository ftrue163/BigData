package com.atguigu.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

/**
 * 这个类是用来消费Kafka的数据的
 */
object MyKafkaUtil {
    //1.创建配置信息对象　　　　　　　
    private val properties: Properties = PropertiesUtil.load("config.properties")

    //2.用于初始化连接到kafka集群的地址
    private val broker_list: String = properties.getProperty("kafka.broker.list")

    //3.Kafka消费者配置
    val kafkaParam = Map(
        "bootstrap.servers" -> broker_list,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "bigdata2021",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    //读取Kafka数据创建DStream
    def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
        kafkaDStream
    }
}
