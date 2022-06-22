package com.atguigu.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;


/**
 * 自定义分区器
 * 使用自定义的分区器，在生产者的配置中添加分区器参数
 *
 */
public class MyKafkaProducerWithMyPartitioner {
    public static void main(String[] args) throws InterruptedException {
        // 1. 创建kafka生产者的配置对象
        Properties props = new Properties();

        // 2. 给kafka配置对象添加配置信息
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        // 添加自定义分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.atguigu.kafka.partitioner.MyPartitioner");


        // 3. 创建kafka生产者对象
        KafkaProducer<String, String> myKafkaProducer = new KafkaProducer<>(props);


        // 4. 调用send方法发送消息
        for (int i = 0; i < 100; i++) {
            myKafkaProducer.send(new ProducerRecord<String, String>("second", "atguigu" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // 判断发送成功与否
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.printf("offset = %s, partition = %s, topic = %s\n", metadata.offset(), metadata.partition(), metadata.topic());
                    }
                }
            });

            //Thread.sleep(2);
        }


        // 5. 关闭资源
        myKafkaProducer.close();
    }
}
