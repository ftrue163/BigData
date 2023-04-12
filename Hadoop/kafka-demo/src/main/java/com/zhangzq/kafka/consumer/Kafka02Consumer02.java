package com.zhangzq.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * 测试一个分区只能被消费者组中的一个消费者消费
 *
 * （1）复制一份基础消费者的代码，在idea中同时启动，即可启动同一个消费者组中的两个消费者。
 * （2）启动代码中的生产者发送消息，即可看到两个消费者在消费不同分区的数据
 *  注意：如果主题分区数为1，可以看到只能有一个消费者消费到数据
 */
public class Kafka02Consumer02 {
    public static void main(String[] args) {
        // 1.创建消费者的配置对象
        Properties properties = new Properties();

        // 2.给消费者配置对象添加参数
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");

        // 配置序列化 必须
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 配置消费者组 必须
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        // 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 注册主题
        ArrayList<String> strings = new ArrayList<>();
        strings.add("first");
        consumer.subscribe(strings);

        // 拉取数据打印
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.printf("offset = %s, topic = %S, partition = %s, value = %s\n", consumerRecord.offset(), consumerRecord.topic(), consumerRecord.partition(), consumerRecord.value());
            }
        }
    }
}
