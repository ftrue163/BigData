package com.atguigu.flinksql.over;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * SQL over窗口函数
 */
public class Flink02_SQL_OverWindow {
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

        //SQL 查询表中数据  使用over窗口函数
        tableEnv.executeSql("select" +
                                "  id," +
                                "  ts," +
                                "  vc," +
                                "  sum(vc) over w, " +
                                "  count(vc) over w " +
                                "from sensor " +
                                "window w as (partition by id order by t)"
                /*"select" +
                        "  id," +
                        "  ts," +
                        "  vc," +
                        "  sum(vc) over (partition by id order by t) " +
                        "from sensor"*/)
                .print();
    }
}
