package com.atguigu.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_OperatorState_BroadcastState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        //获取两个流
        DataStreamSource<String> streamSource1 = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> streamSource2 = env.socketTextStream("localhost", 8888);

        //定义广播状态
        MapStateDescriptor<String, String> stringStringMapStateDescriptor = new MapStateDescriptor<>("broad-state", String.class, String.class);

        //创建广播流 来 广播状态
        BroadcastStream<String> broadcastStream = streamSource1.broadcast(stringStringMapStateDescriptor);

        //用 dataStream 连接 broadStream
        BroadcastConnectedStream<String, String> broadcastConnectedStream = streamSource2.connect(broadcastStream);

        //使用BroadcastConnectedStream的process方法
        broadcastConnectedStream.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(stringStringMapStateDescriptor);

                //提取状态中的数据
                String aSwitch = broadcastState.get("switch");

                if ("1".equals(aSwitch)) {
                    out.collect("写入HBase");
                } else if ("2".equals(aSwitch)) {
                    out.collect("写入kafka");
                } else {
                    out.collect("写入文件");
                }
            }

            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                //提取状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stringStringMapStateDescriptor);

                //向状态中存入数据
                broadcastState.put("switch", value);
            }
        }).print();

        env.execute();
    }
}
