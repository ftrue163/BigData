package com.atguigu.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_TransForm_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.分别获取两个流
        DataStreamSource<String> streamSource1 = env.fromElements("a", "b", "c", "d", "d");

        DataStreamSource<Integer> streamSource2 = env.fromElements(1, 2, 3, 4, 5, 6);

        //利用Connect连接两条流 同床异梦
        ConnectedStreams<String, Integer> connectedStreams = streamSource1.connect(streamSource2);

        SingleOutputStreamOperator<String> process = connectedStreams.process(new CoProcessFunction<String, Integer, String>() {
            @Override
            public void processElement1(String value, CoProcessFunction<String, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
            }

            @Override
            public void processElement2(Integer value, CoProcessFunction<String, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value.toString());
            }
        });


        process.print();

        env.execute();
    }
}
