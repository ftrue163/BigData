package com.atguigu.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink08_Window_Time_Tumbling_AggFun {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为Tuple类型的数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                out.collect(Tuple2.of(value, 1));
            }
        });

        //4.将相同单词的数据聚和到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        //5.开启一个基于时间的滚动窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //调用增量聚合函数 AggFun 进行计算
        window.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
            /**
             * 创建累加器（初始化累加器）
             *
             * @return
             */
            @Override
            public Integer createAccumulator() {
                System.out.println("初始化累加器。。。");
                return 0;
            }

            /**
             * 累加操作
             *
             * @param value
             * @param accumulator
             * @return
             */
            @Override
            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                System.out.println("累加操作。。。");
                return value.f1 + accumulator;
            }

            /**
             * 获取累加结果
             *
             * @param accumulator
             * @return
             */
            @Override
            public Integer getResult(Integer accumulator) {
                System.out.println("获取结果。。。");
                return accumulator;
            }

            /**
             * 合并累加器，此方法只用于会话窗口
             *
             * @param a
             * @param b
             * @return
             */
            @Override
            public Integer merge(Integer a, Integer b) {
                System.out.println("merge。。。");
                return a + b;
            }
        }).print();

        env.execute();
    }
}
