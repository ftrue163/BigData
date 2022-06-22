package com.atguigu.flinksql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink01_TableAPI_Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        //创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //对动态表进行连续查询 得到新的动态表
        Table resultTable = table.select($("id"), $("ts"), $("vc"));

        //将动态表转换成追加流
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(resultTable, Row.class);
        //和上一行的效果一样
        //DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        //打印
        rowDataStream.print();

        env.execute();
    }
}
