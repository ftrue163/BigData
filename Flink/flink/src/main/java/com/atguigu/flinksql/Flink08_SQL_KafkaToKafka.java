package com.atguigu.flinksql;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class Flink08_SQL_KafkaToKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建读Kafka数据的表
        tableEnv.executeSql("create table source_sensor (" +
                "  id string, ts bigint, vc int" +
                ") with (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'topic_source_sensor'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                "  'properties.group.id' = 'atguigu'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'csv'" +
                ")");

        //创建写Kafka数据的表
        tableEnv.executeSql("create table sink_sensor (" +
                "  id string, ts bigint, vc int" +
                ") with (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'topic_sink_sensor'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                "  'format' = 'csv'" +
                ")");

        //将从kafka表中的数据写入另一张kafka的表中
        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id = 'sensor_1'");
    }

}
