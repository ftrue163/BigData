package com.atguigu.practice;


import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 6.3	各省份页面广告点击量实时统计
 */
public class Flink04_Project_Ads_Click {
    /*public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.readTextFile("flink/input/AdClickLog.csv");

        SingleOutputStreamOperator<AdsClickLog> map = streamSource.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] split = value.split(",");
                return new AdsClickLog(
                        Long.valueOf(split[0]),
                        Long.valueOf(split[1]),
                        split[2],
                        split[3],
                        Long.valueOf(split[4])

                );
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> map1 = map.map(log -> Tuple2.of(log.getProvince() + "_" + log.getAdId(), 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> keyedStream = map1.keyBy(tuple2 -> tuple2.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        sum.print();

        env.execute();
    }*/


    /**
     * 参考答案
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("flink/input/AdClickLog.csv");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<AdsClickLog> adsClickLogDStream = streamSource.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] split = value.split(",");
                return new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });

        //4.将数据组成Tuple元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = adsClickLogDStream.map(new MapFunction<AdsClickLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(AdsClickLog value) throws Exception {
                return Tuple2.of(value.getProvince() + "-" + value.getAdId(), 1);
            }
        });

        //5.将相同key的数据聚和到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = streamOperator.keyBy(0);

        //6.累加操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        env.execute();


    }
}
