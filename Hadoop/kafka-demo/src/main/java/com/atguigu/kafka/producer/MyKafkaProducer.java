package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 异步发送API
 *
 * 不带回调函数的API
 */
public class MyKafkaProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //设置配置文件内容：1.需要配置kafka集群地址:该地址可以找到我们的kafka集群 一般工作中写两个地址：hadoop102:9092,hadoop103:9092，以防其中一个broker挂掉
        //2.配置序列化器：key的序列化器    value序列化器的：一般情况下：都是String，因为String这个数据类型Java提供的api相对较多，而且，我们需要注意的是：kafka的序列化器，一般要使用官方提供的序列化器
        /*
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        */
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 批次大小 默认16384byte 即16K
        props.put("batch.size", 16384);

        // 等待时间
        //linger.ms=0  默认是0   0意味着不等待  一般工作中使用kafka的时候设置为0，因为我们kafka的使用场景通常为实时场景
        props.put("linger.ms", 1);

        // RecordAccumulator缓冲区大小 默认33554432字节 即32M
        props.put("buffer.memory", 33554432);



        //new 一个kafka的生产者 该生产者需要 properties文件
        KafkaProducer<String, String> myKafkaProducer = new KafkaProducer<>(props);

        for (int i = 0; i < 100; i++) {
            //封装我们要发送的消息内容
            //如果topic first在kafka集群中不存在，则会自动创建此主题，分区数为1，副本数为1
            ProducerRecord<String, String> myProducerRecord = new ProducerRecord<>("first", "atguigu-" + i);
            //调用生产者send方法将我们封装好的消息发送出去
            myKafkaProducer.send(myProducerRecord);

        }

        //关闭KafkaProducer客户端
        //生命周期函数，它的底层都带有一个flush()函数  这样就会把我们数据书写出去
        myKafkaProducer.close();
    }
}
