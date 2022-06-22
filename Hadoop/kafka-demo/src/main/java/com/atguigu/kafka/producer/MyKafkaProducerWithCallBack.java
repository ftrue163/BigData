package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 *
 * 异步发送API
 *
 * 带回调函数的API
 * 回调函数会在producer收到ack时调用，为异步调用，该方法有两个参数，分别是RecordMetadata和Exception，如果Exception为null，说明消息发送成功，如果Exception不为null，说明消息发送失败。
 * 注意：消息发送失败会自动重试，不需要我们在回调函数中手动重试。
 */
public class MyKafkaProducerWithCallBack {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 设置ack  默认1
        props.put("acks", "all");

        // 重试次数  默认2147483647
        props.put("retries", 3);

        // 批次大小 默认16K
        props.put("batch.size", 16384);

        // 等待时间 默认0
        props.put("linger.ms", 1);

        // RecordAccumulator缓冲区大小 默认32M
        props.put("buffer.memory", 33554432);


        //自定义分区器的使用！
        // pros.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.atguigu.kafka.partitioner.Mypartititoner");



        KafkaProducer<String, String> myKafkaProducer = new KafkaProducer<>(props);

        for (int i = 0; i < 100; i++) {
            //先在kafka shell命令行中创建了一个second 的topic 分区数为5
            //kafka-topics.sh --bootstrap-server hadoop102:9092 --create --topic second --partitions 5
            ProducerRecord<String, String> myProducerRecord = new ProducerRecord<>("second", "atguigu-" + i);

            // 添加回调
            myKafkaProducer.send(myProducerRecord, new Callback() {
                // 该方法在Producer收到ack时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        // 出现异常打印
                        System.out.println("kafka生产者 发送消息不成功！");
                        exception.printStackTrace();
                    } else {
                        // 没有异常,输出信息到控制台
                        System.out.printf("offset = %s, partition = %s, topic = %s, value = %s\n", metadata.offset(), metadata.partition(), metadata.topic(), myProducerRecord.value());
                    }
                }
            });

            //使用线程睡眠时间 使我们发送数据的时候，不按照批次发送的条件来发送消息
            Thread.sleep(100);
        }

        myKafkaProducer.close();
    }
}
