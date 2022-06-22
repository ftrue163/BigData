package com.atguigu.flinksql.connect;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink03_TableAPI_Connect_File_Source {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建Schema
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        //通过File Connect连接器 创建 临时表
        tableEnv.connect(new FileSystem().path("flink/input/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //将表执行环境中的临时表 创建为 动态表
        Table table = tableEnv.from("sensor");

        //对动态表进行连续查询 获得新的动态表
        //方式1  table
        //Table resultTable = table.groupBy($("id"))
                //.select($("id"), $("vc").sum().as("vcSum"));
        //方式2  sql
        Table resultTable = tableEnv.sqlQuery("select id, sum(vc) vcSum from sensor group by id");


        //将动态表 转换为 追加流
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(resultTable, Row.class);


        //打印流
        rowDataStream.print();

        env.execute();
    }
}
