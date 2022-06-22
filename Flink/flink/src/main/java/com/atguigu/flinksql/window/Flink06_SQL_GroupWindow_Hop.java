package com.atguigu.flinksql.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * SQL  事件时间   分组  滑动窗口
 */
public class Flink06_SQL_GroupWindow_Hop {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //SQL 注册File表
        tableEnv.executeSql("create table sensor (" +
                "  id string," +
                "  ts bigint," +
                "  vc int," +
                "  t as to_timestamp(from_unixtime(ts / 1000))," +
                "  watermark for t as t - interval '5' second" +
                ") with (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'flink/input/sensor-sql.txt'," +
                "  'format' = 'csv'" +
                ")");

        //SQL 查询动态表并打印
        //开启一个基于事件时间的滑动窗口
        //当读取有界数据时，使用处理时间的话会导致窗口无法关闭，因为没有数据输出
        tableEnv.executeSql("select " +
                "  id," +
                "  hop_start(t, interval '2' second, interval '3' second) as wStart," +
                "  hop_end(t, interval '2' second, interval '3' second) as wEnd," +
                "  sum(vc) sum_vc " +
                "from sensor " +
                "group by hop(t, interval '2' second, interval '3' second), id").print();
    }
}
