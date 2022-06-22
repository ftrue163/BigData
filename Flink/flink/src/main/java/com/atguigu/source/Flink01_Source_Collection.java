package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从集合中获取数据
        DataStreamSource<Integer> streamSource = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));

        //从端口中获取数据
        //DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //多并行度方法，核心是实现了ParallelSourceFunction这个接口
        //env.fromParallelCollection()

        env.execute();
    }
}
