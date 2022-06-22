package com.atguigu.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink06_TransForm_Union {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.分别获取两个流
        DataStreamSource<String> streamSource1 = env.fromElements("a", "b", "c", "d", "e");

        DataStreamSource<String> streamSource2 = env.fromElements("1","2","3","4","5","6");

        DataStreamSource<String> streamSource3 = env.fromElements("z","x","c","v");

        //利用Union连接两条流 水乳交融
        DataStream<String> union = streamSource1.union(streamSource2, streamSource3);

        SingleOutputStreamOperator<String> process = union.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
            }
        });

        process.print();

        env.execute();
    }
}
