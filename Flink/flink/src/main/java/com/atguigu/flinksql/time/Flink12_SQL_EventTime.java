package com.atguigu.flinksql.time;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 文件系统  事件时间   sql
 */
public class Flink12_SQL_EventTime {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //指定时区配置
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.local-time-zone", "GMT");

        //sql方式 创建File类型的表  指定event_time和watermark
        tableEnv.executeSql("create table sensor (" +
                "  id string," +
                "  ts bigint," +
                "  vc int," +
                "  event_time as to_timestamp(from_unixtime(ts / 1000))," +
                "  watermark for event_time as  event_time - interval '2' second" +
                ") with (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'flink/input/sensor-sql.txt'," +
                "  'format' = 'csv'" +
                ")");

        //sql方式 查询表中的数据
        tableEnv.executeSql("select * from sensor").print();
    }
}
