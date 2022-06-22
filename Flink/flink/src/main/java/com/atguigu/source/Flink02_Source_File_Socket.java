package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_Source_File_Socket {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //从文件中获取数据
        //DataStreamSource<String> streamSource = env.readTextFile("flink/input/word.txt");

        //从端口中获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        streamSource.print();

        env.execute();
    }
}
