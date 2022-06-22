package com.atguigu.flinksql.time;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * 文件系统  处理时间  sql
 */
public class Flink11_SQL_ProcessingTime {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //在建表时指定处理时间字段
        tableEnv.executeSql("create table sensor (" +
                "  id string," +
                "  ts bigint," +
                "  vc int," +
                "  pt_time as PROCTIME()" +
                ") with (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'flink/input/sensor-sql.txt'," +
                "  'format' = 'csv'" +
                ")");

        tableEnv.executeSql("select * from sensor").print();
    }
}
