package com.atguigu.flinksql.connect;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

public class Flink05_TableAPI_Connect_File_Sink {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        //获取表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建Schema
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        //创建File类型的临时表
        tableEnv.connect(new FileSystem().path("flink/out/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter('|'))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorStream);

        //对动态表进行连续查询 获得新的动态表
        Table resultTable = table.where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //将动态表的数据 写入 临时表（即文件）中
        resultTable.executeInsert("sensor");

        //No operators defined in streaming topology. Cannot execute.
        //env.execute();
    }
}
