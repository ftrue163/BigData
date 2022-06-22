package com.atguigu.flinksql.over;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.rowInterval;


/**
 * Table API over窗口函数
 */
public class Flink01_TableAPI_OverWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转换为表 并指定事件时间字段
        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts").rowtime(), $("vc"));

        //查询表中数据，并开启over窗口
        //注意：使用over开窗时，必须指定order by，对于流处理，order by字段必须是时间字段
        table
                //从开始时间 到 当前时间
                //.window(Over.partitionBy($("id")).orderBy($("ts")).as("w"))
                //往前推算两行
                //.window(Over.partitionBy($("id")).orderBy($("ts")).preceding(rowInterval(2L)).as("w"))
                //往前推算两秒
                //.window(Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(2).second()).as("w"))
                .select($("id"), $("ts"), $("vc"), $("vc").sum().over($("w")).as("vcSum"))
                .execute().print();
    }
}
