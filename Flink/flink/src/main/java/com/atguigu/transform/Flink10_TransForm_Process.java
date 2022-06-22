package com.atguigu.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink10_TransForm_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.使用process实现Map方法
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, ProcessFunction<String, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(" ");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        //利用Process实现累加器功能
        SingleOutputStreamOperator<Integer> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, Integer>() {
            private Integer count = 0;

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                count++;
                out.collect(count);
            }
        });


        result.print();
        env.execute();
    }
}
