package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;


/**
 * 分区的原则
 * 我们需要将producer发送的数据封装成一个ProducerRecord对象。
 * （1）  指明 partition 的情况下，直接将指明的值直接作为 partiton 值；
 * （2） 没有指明 partition 值但有 key 的情况下，将 key 的 hash 值与 topic 的 partition 数进行取余得到 partition 值；
 * （3）  既没有 partition 值又没有 key 值的情况下， kafka采用Sticky Partition(黏性分区器)，会随机选择一个分区，并尽可能一直使用该分区，待该分区的batch已满或者已完成，kafka再随机一个分区进行使用.(以前是一条条的轮询，现在是一批次的轮询)
 *
 */
public class MyKafkaProducerWithPartition {
    public static void main(String[] args) throws InterruptedException {
        // 1. 创建kafka生产者的配置对象
        Properties props = new Properties();

        // 2. 给kafka配置对象添加配置信息
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        // 3. 创建kafka生产者对象
        KafkaProducer<String, String> myKafkaProducer = new KafkaProducer<>(props);


        // 4. 调用send方法发送消息
        for (int i = 0; i < 10; i++) {
            // 指定发送到1号分区
            myKafkaProducer.send(new ProducerRecord<>("second", 1, "", "atguigu" + i));
            // 线程睡眠,避免全部发送到一个分区
            Thread.sleep(2);
        }

        // 4. 调用send方法发送消息
        for (int i = 0; i < 10; i++) {
            // 根据key的hash值分配分区
            myKafkaProducer.send(new ProducerRecord<>("second", "abc", "atguigu" + i));
            // 提供线程睡眠,避免发送到同一个分区
            Thread.sleep(2);
        }


        // 5. 关闭资源
        myKafkaProducer.close();
    }
}
