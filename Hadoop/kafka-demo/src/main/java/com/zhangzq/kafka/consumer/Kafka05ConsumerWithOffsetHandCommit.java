package com.zhangzq.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;


/**
 * 虽然自动提交offset十分简介便利，但由于其是基于时间提交的，开发人员难以把握offset提交的时机。因此Kafka还提供了手动提交offset的API。
 *
 * 手动提交offset的方法有两种：分别是commitSync（同步提交）和commitAsync（异步提交）。
 *      两者的相同点是，都会将本次poll的一批数据最高的偏移量提交；
 *      不同点是，commitSync阻塞当前线程，一直到提交成功，并且会自动失败重试（由不可控因素导致，也会出现提交失败）；
 *              而commitAsync则没有失败重试机制，故有可能提交失败。
 *
 * 同步提交：
 *      由于同步提交offset有失败重试机制，故更加可靠。
 *
 * 异步提交：
 *      虽然同步提交offset更可靠一些，但是由于其会阻塞当前线程，直到提交成功。因此吞吐量会受到很大的影响。因此更多的情况下，会选用异步提交offset的方式。
 *
 * 数据漏消费和重复消费分析
 *      无论是同步提交还是异步提交offset，都有可能会造成数据的漏消费或者重复消费。
 *      先提交offset后消费，有可能造成数据的漏消费；而先消费后提交offset，有可能会造成数据的重复消费。
 */
public class Kafka05ConsumerWithOffsetHandCommit {
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
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
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

            // 同步提交offset
            // consumer.commitSync();

            // 异步提交offset
            consumer.commitAsync(new OffsetCommitCallback() {
                /**
                 * 回调函数输出
                 * @param offsets   offset信息
                 * @param exception 异常
                 */
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    // 如果出现异常打印
                    if (exception != null ){
                        System.err.println("Commit failed for " + offsets);
                    }
                }
            });
        }
    }
}
