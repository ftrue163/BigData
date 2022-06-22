package com.atguigu.keyby;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeyByAndAgg {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //并行度设置为1
        //env.setParallelism(1);

        //全局都断开（算子链）
        //env.disableOperatorChaining();

        //获取有界数据（文件中的数据）
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //将每一行数据按照空格切分出每一个单词
        SingleOutputStreamOperator<String> wordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //按照空格进行切分
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        })
                //断开前面的算子链
                //.startNewChain()
                //与前后都断开
                //.disableChaining()
                //.slotSharingGroup("group1")
                //.setParallelism(5)
                ;

        //将单词组成Tuple2元组
        /*SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = wordDStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });*/

        //如果使用lambda表达式，可能因为 类型擦除 报错
        //解决： returns(Types.类型)
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = wordDStream.map(word -> Tuple2.of(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT)); //当Lambda表达式使用 java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息

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
