package com.atguigu.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink04_Sink_Custom_Mysql {
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
        waterSensorDStream.addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<WaterSensor> {
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            //创建连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456zzQ!?#");

            //创建语句预执行者
            preparedStatement = connection.prepareStatement("insert into sensor values (?, ?, ?)");
        }

        @Override
        public void close() throws Exception {
            //关闭连接
            preparedStatement.close();
            connection.close();
        }

        /**
         * SinkFunction 的核心方法
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(WaterSensor value, SinkFunction.Context context) throws Exception {
            //给占位符赋值
            preparedStatement.setString(1, value.getId());
            preparedStatement.setLong(2, value.getTs());
            preparedStatement.setInt(3, value.getVc());

            preparedStatement.execute();
        }
    }
}
