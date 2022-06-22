package com.atguigu.flinksql.window;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.rowInterval;


/**
 * Table API  分组  计数滚动窗口
 */
public class Flink04_TableAPI_GroupWindow_Tumbling_Count {
    public static void main(String[] args) {
        //1.流执行环境
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

        //获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转换为动态表 并指定eventtime和watermark
        Table table = tableEnv.fromDataStream(waterSensorStream, $("id"), $("ts"), $("vc"), $("pt").proctime());

        //Table API方式 对动态表进行分组  开滑动窗口
        table.window(Tumble.over(rowInterval(2L)).on($("pt")).as("w"))
                .groupBy($("id"), $("w"))
                .select($("id"), $("vc").sum().as("vcSum"))
                .execute().print();
    }
}
