package com.atguigu.practice;


import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.util.hash.Hash;

import java.util.HashSet;


/**
 * 需求：网站独立访客数（UV）的统计
 */
public class Flink02_Project_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.readTextFile("flink/input/UserBehavior.csv");

        //不仅有转换和扩展的作用，还有过滤的作用
        SingleOutputStreamOperator<Tuple2<String, Long>> uv = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] split = value.split(",");
                if ("pv".equals(split[3])) {
                    out.collect(Tuple2.of("uv", Long.valueOf(split[0])));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, String> keyedStream = uv.keyBy(tuple2 -> tuple2.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> process = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>() {
            private HashSet<Long> userIds = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                userIds.add(value.f1);
                out.collect(Tuple2.of(value.f0, (long) userIds.size()));
            }
        });

        process.print("pv");  //结果：215662

        env.execute();
    }


    //参考答案
    /*public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .readTextFile("flink/input/UserBehavior.csv")
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] split = line.split(",");
                    UserBehavior behavior = new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                    if ("pv".equals(behavior.getBehavior())) {
                        out.collect(Tuple2.of("uv", behavior.getUserId()));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {
                    HashSet<Long> userIds = new HashSet<>();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                        userIds.add(value.f1);
                        out.collect(userIds.size());
                    }
                })
                .print("uv");  //结果：215662

        env.execute();
    }*/

}
