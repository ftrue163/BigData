package com.atguigu.flinksql.connect;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class Flink04_TableAPI_Connect_Kafka_Source {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建Schema
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        //通过 Kafka Connector 和 表环境 创建临时表
        tableEnv.connect(new Kafka()
                        .topic("sensor")
                        .startFromLatest()
                        .version("universal")
                        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                        .property(ConsumerConfig.GROUP_ID_CONFIG, "0625"))
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");


        //将表执行环境中的临时表 创建为 动态表
        Table table = tableEnv.from("sensor");

        //对动态表进行连续查询 获得新的动态表
        //方式1  table
        Table resultTable = table.groupBy($("id"))
                .select($("id"), $("vc").sum().as("vcSum"));
        //方式2  sql
        //Table resultTable = tableEnv.sqlQuery("select id, sum(vc) vcSum from sensor group by id");


        //将动态表 转换为 追加流
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(resultTable, Row.class);


        //打印流
        rowDataStream.print();

        env.execute();
    }
}
