package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 同步发送API
 *
 * 同步发送的意思就是，一条消息发送之后，会阻塞当前线程，直至返回ack。
 * 由于send方法返回的是一个Future对象，根据Futrue对象的特点，我们也可以实现同步发送的效果，只需再调用Future对象的get方法即可。
 */
public class MyKafkaProducerWithCallBackSync {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // 1. 创建kafka生产者的配置对象
        Properties props = new Properties();

        // 2. 给kafka配置对象添加配置信息
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 设置ack
        props.put("acks", "all");

        // 重试次数
        props.put("retries", 3);

        // 批次大小 默认16K
        props.put("batch.size", 16384);

        // 等待时间
        props.put("linger.ms", 1);

        // RecordAccumulator缓冲区大小 默认32M
        props.put("buffer.memory", 33554432);


        // 3. 创建kafka生产者对象
        KafkaProducer<String, String> myKafkaProducer = new KafkaProducer<>(props);

        // 4. 调用send方法,发送消息
        for (int i = 0; i < 10; i++) {

            ProducerRecord<String, String> myProducerRecord = new ProducerRecord<>("first", "atguigu-" + i);

            // 异步发送 默认
            //myKafkaProducer.send(myProducerRecord);


            // 同步发送
            //在调用send方法时 同时调用get()方法, 能够达到同步发送的效果，但是！！！！我们在使用kafka的过程中 绝大部分情况不会这样使用，因为Kafka是一个异步架构，
            //在出现排查问题逻辑的时候，可能会用到。
            myKafkaProducer.send(myProducerRecord).get();
        }

        // 5. 关闭资源
        myKafkaProducer.close();
    }
}
