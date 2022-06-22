package com.atguigu.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_TransForm_Max_MaxBy {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });


        //waterSensorDStream.keyBy(r -> r.getId());
        //waterSensorDStream.keyBy(WaterSensor::getId);
        //waterSensorDStream.keyBy("id");
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });


        //使用简单滚动聚合算子
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.max("vc");
        //SingleOutputStreamOperator<WaterSensor> result = keyedStream.maxBy("vc", false);
        //SingleOutputStreamOperator<WaterSensor> result = keyedStream.maxBy("vc", true);

        result.print();

        env.execute();
    }
}
