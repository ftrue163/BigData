package com.atguigu.cep;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
 */
public class Flink12_CEP_Project_OrderWatch {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据
        KeyedStream<OrderEvent, Tuple> keyedStream = env.readTextFile("flink/input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.getEventTime() * 1000;
                                    }
                                })
                )
                .keyBy("orderId");

        //定义模式
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("start")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .next("end")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        //将模式作用于流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //获取匹配到的数据
        SingleOutputStreamOperator<String> selectResult = patternStream.select(new OutputTag<String>("output") {
        }, new PatternTimeoutFunction<OrderEvent, String>() {
            @Override
            public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                return pattern.toString();
            }
        }, new PatternSelectFunction<OrderEvent, String>() {
            @Override
            public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                return pattern.toString();
            }
        });


        //打印主流
        //selectResult.print("主流");

        //获取测输出流（超时数据）并打印输出
        selectResult.getSideOutput(new OutputTag<String>("output") {}).print("超时");

        env.execute();
    }
}
