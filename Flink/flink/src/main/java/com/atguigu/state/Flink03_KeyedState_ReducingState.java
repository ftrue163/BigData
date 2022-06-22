package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * KeyedState_ReducingState
 * 	计算每个传感器的水位和
 */
public class Flink03_KeyedState_ReducingState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据并转为JavaBean->判断连续两个水位差值是否超过10
        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //定义一个ReducingState键控状态 用来计算水位和
                    private ReducingState<Integer> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态
                        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("reducing-state", Integer::sum, Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //将当前数据存入状态中做计算
                        reducingState.add(value.getVc());

                        //取出计算结果
                        Integer vcSum = reducingState.get();

                        out.collect(value.getId() + "-" + vcSum);
                    }
                }).print();

        env.execute();
    }
}
