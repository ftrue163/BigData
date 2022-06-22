package com.atguigu;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //创建批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //获取文件中的数据
        DataSource<String> dataSource = env.readTextFile("flink/input/word.txt");

        //按照空格切分，切分出每一个单词
        FlatMapOperator<String, String> word = dataSource.flatMap(new MyFlatMap());

        //使用map将单词组成Tuple2元组
        MapOperator<String, Tuple2<String, Integer>> wordToOne = word.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
                //return new Tuple2<>(value, 1);
            }
        });

        //将相同key的数据聚和到一块(通过下标的方式指定key)
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        //对value进行相加（通过下标的方式）
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        //打印到控制台
        result.print();
    }

    public static class MyFlatMap implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            //按照空格进行切分
            for (String word : value.split(" ")) {
                out.collect(word);
            }
        }
    }
}
