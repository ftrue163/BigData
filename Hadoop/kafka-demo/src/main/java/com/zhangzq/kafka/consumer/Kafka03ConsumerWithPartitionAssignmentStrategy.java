package com.zhangzq.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * 分区分配策略
 *      一个consumer group中有多个consumer，一个 topic有多个partition，所以必然会涉及到partition的分配问题，即确定那个partition由哪个consumer来消费。
 *
 *  Kafka有两种分配策略，RoundRobin，Range。
 *      1）RoundRobin
 *      2）Range
 *          ① 修改主题first为7个分区
 *          ② 复制基础消费者一个三个，消费者组都是“test”，同时启动3个消费者。
 *          ③ 启动生产者，发送500条消息，随机发送到不同的分区：
 *          ④ 观看3个消费者分别消费哪些分区的数据
 *      3）Sticky
 *          特殊的分配策略StickyAssignor，Kafka从0.11.x版本开始引入这种分配策略，在出现同一消费者组内消费者出现问题的时候，会进行使用。
 *
 *  默认使用Range的分区分配策略，可以通过参数"partition.assignment.strategy"的值进行修改。
 *  注意：3个消费者都应该修改分区分配策略，避免出现错误，如果重启失败，则全部停止消费者等一会再启动即可
 *
 *
 */
public class Kafka03ConsumerWithPartitionAssignmentStrategy {
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

        // 修改分区分配策略（注意：3个消费者都应该修改分区分配策略）
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");

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
