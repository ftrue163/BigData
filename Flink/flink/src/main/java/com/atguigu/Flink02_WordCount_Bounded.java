package com.atguigu;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //设置执行模式（Execution Mode）
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


        //获取有界数据（文件中的数据）
        DataStreamSource<String> streamSource = env.readTextFile("flink/input/word.txt");

        //将每一行数据按照空格切分出每一个单词
        SingleOutputStreamOperator<String> wordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //按照空格进行切分
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        });

        //将单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = wordDStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //将相同单词的数据聚和到一块
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });


        //对value相加
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        sum.print();

        //执行任务
        env.execute();
    }
}
