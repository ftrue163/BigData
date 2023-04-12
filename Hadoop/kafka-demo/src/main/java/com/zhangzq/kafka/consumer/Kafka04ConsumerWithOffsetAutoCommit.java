package com.zhangzq.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


/**
 * 由于consumer在消费过程中可能会出现断电宕机等故障，consumer恢复后，需要从故障前的位置的继续消费，所以consumer需要实时记录自己消费到了哪个offset，以便故障恢复后继续消费。
 * Kafka 0.9版本之前，consumer默认将offset保存在Zookeeper中，从0.9版本开始，consumer默认将offset保存在Kafka一个内置的topic中，该topic为__consumer_offsets。
 *
 * 自动提交offset
 *      为了使我们能够专注于自己的业务逻辑，Kafka提供了自动提交offset的功能。
 *          自动提交offset的相关参数：
 *              enable.auto.commit：是否开启自动提交offset功能 周期性自动提交 默认是开启的
 *              auto.commit.interval.ms：自动提交offset的时间间隔   默认5000 即5s
 *
 * 重置Offset
 *      auto.offset.reset = earliest | latest | none
 *      当Kafka中没有初始偏移量（消费者组第一次消费）或服务器上不再存在当前偏移量时（例如该数据已被删除），该怎么办：
 *          （1）earliest：自动将偏移量重置为最早的偏移量
 *          （2）latest(默认值)：自动将偏移量重置为最新偏移量
 *          （3）none：如果未找到消费者组的先前偏移量，则向消费者抛出异常
 */
public class Kafka04ConsumerWithOffsetAutoCommit {
    public static void main(String[] args) {
        // 1. 创建kafka消费者配置类
        Properties properties = new Properties();
        // 2. 添加配置参数
        // 添加连接
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");

        // 配置序列化 必须
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 配置消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 是否自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 提交offset的时间周期
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // 3. 创建kafka消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 4. 设置消费主题  形参是列表
        consumer.subscribe(Arrays.asList("first"));

        // 5. 消费数据
        while (true) {
            // 读取消息
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

            // 输出消息
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.value());
            }
        }
    }
}
