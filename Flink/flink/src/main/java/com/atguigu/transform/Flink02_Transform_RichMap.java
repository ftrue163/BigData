package com.atguigu.transform;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_Transform_RichMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //DataStreamSource<String> streamSource = env.readTextFile("flink/input/word.txt");
        //DataStreamSource<String> streamSource = env.fromElements("1", "2", "3", "4");

        //3.使用map算子
        SingleOutputStreamOperator<String> map = streamSource.map(new MyMap());

        map.print();

        env.execute();
    }

    public static class MyMap extends RichMapFunction<String, String> {
        /**
         * 在程序最开始的时候调用
         * 每个并行度调用一次
         * 生命周期开启
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }

        /**
         * 在程序结束的时候调用
         * 每个并行度调用一次（当读文件时，每个并行度调用两次）
         * 生命周期结束
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }


        @Override
        public String map(String value) throws Exception {
            System.out.println(getRuntimeContext().getTaskName());
            System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
            return value;
        }
    }
}
