package com.atguigu.flinksql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.CURRENT_ROW;

public class Flink02_TableAPI_Demo_Agg {
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

        //对动态表进行连续查询（聚合查询） 获取新的动态表
        Table resultTable = table
                /*.groupBy($("id"))
                .aggregate($("vc").sum().as("vcSum"))
                .select($("id"), $("vcSum"));*/
                //在select中直接做聚合操作
                .groupBy($("id"))
                .select($("id"), $("vc").sum().as("vcSum"));

        //将动态表转换为撤回流
        //错误演示：toAppendStream doesn't support consuming update changes which is produced by node GroupAggregate(groupBy=[id], select=[id, SUM(vc) AS TMP_0])
        //DataStream<Row> rowDataStream = tableEnv.toAppendStream(resultTable, Row.class);
        //此时必须是toRetractStream
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(resultTable, Row.class);

        //打印流
        rowDataStream.print();

        env.execute();
    }
}
