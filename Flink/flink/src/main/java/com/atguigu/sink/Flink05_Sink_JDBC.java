package com.atguigu.sink;

import com.atguigu.bean.WaterSensor;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * JDBC sink: 适合于单表的数据写入，因为如果多张表的话，每张表的字段个数和字段类型可能不一样
 *
 *
 */
public class Flink05_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //自定义Sink将数据写入Mysql
        waterSensorDStream.addSink(JdbcSink.sink("insert into sensor values (?, ?, ?)",
                (ps, t) -> {
                    ps.setString(1, t.getId());
                    ps.setLong(2, t.getTs());
                    ps.setInt(3, t.getVc());
                },
                //来一条写一条
                new JdbcExecutionOptions.Builder().withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(Driver.class.getName())
                        .withPassword("123456zzQ!?#")
                        .withUsername("root")
                        .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                        .build()
        ));

        env.execute();
    }
}
