package com.atguigu.flinksql;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 查询未注册的表 和 查询已注册的表
 *
 */
public class Flink07_SQL_Demo {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从元素中读取数据
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //查询未注册的表
        //方式1
        /*Table resultTable = tableEnv.sqlQuery("select * from " + table + " where id = 'sensor_1'");
        //将动态表转换为追加流
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(resultTable, Row.class);
        rowDataStream.print();*/
        //方式2
        /*Table resultTable = tableEnv.sqlQuery("select * from " + table + " where id = 'sensor_1'");
        TableResult tableResult = resultTable.execute();
        tableResult.print();*/
        //方式3
        /*TableResult tableResult = tableEnv.executeSql("select * from " + table + " where id = 'sensor_1'");
        tableResult.print();*/


        //查询已注册的表
        //tableEnv.registerTable("sensor", table);
        //tableEnv.createTemporaryView("sensor", table);
        tableEnv.createTemporaryView("sensor", waterSensorStream);
        //TableResult tableResult = tableEnv.executeSql("select * from sensor where id = 'sensor_1'");
        //tableResult.print();

        TableResult tableResult = tableEnv.executeSql("select id, sum(vc) vcSum from sensor sensor group by id");
        tableResult.print();

        //env.execute();
    }
}
