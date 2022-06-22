package com.atguigu;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_Application_PerJob_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test1(env);
        test2(env);
        test3(env);
    }

    public static void test1(StreamExecutionEnvironment env) throws Exception {

        /*DataStreamSource<String> stringDataStreamSource = env.fromElements("22222");
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();*/

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("hadoop102", 9999);
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();
    }

    public static void test2(StreamExecutionEnvironment env) throws Exception {
        /*
        DataStreamSource<String> stringDataStreamSource = env.fromElements("22222");
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();*/

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("hadoop102", 9999);
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();
    }

    public static void test3(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("hadoop102", 9999);
        stringDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();
    }
}
