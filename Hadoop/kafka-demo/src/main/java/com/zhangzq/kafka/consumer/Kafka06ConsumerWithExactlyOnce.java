package com.zhangzq.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;


/**
 * 事务机制主要是从Producer方面考虑，对于Consumer而言，事务的保证就会相对较弱，尤其时无法保证Commit的信息被精确消费。
 * 这是由于Consumer可以通过offset访问任意信息，而且不同的Segment File生命周期不同，同一事务的消息可能会出现重启后被删除的情况。
 *
 * 如果想完成Consumer端的精准一次性消费，那么需要kafka消费端将消费过程和提交offset过程做原子绑定。
 * 此时我们需要将kafka的offset保存到支持事务的自定义介质（比如mysql）。这部分知识会在后续项目部分涉及。
 */
public class Kafka06ConsumerWithExactlyOnce {
    public static void main(String[] args) {

    }
}
