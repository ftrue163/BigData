package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 去重: 去掉重复的水位值.
 * 思路: 把水位值作为MapState的key来实现去重, value随意
 */
public class Flink05_KeyedState_MapState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据并转为JavaBean->判断连续两个水位差值是否超过10
        KeyedStream<WaterSensor, Tuple> keyedStream = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .keyBy("id");

        //
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //定义一个MapState键控状态来保存vc
            private MapState<Integer, WaterSensor> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>("map-state", Integer.class, WaterSensor.class));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                //判断状态中是否有当前vc
                if (!mapState.contains(value.getVc())) {
                    //没有相同的key
                    //将当前的vc保存到状态中
                    mapState.put(value.getVc(), value);
                }

                Iterable<WaterSensor> values = mapState.values();

                out.collect(values.toString());
            }
        }).print();

        env.execute();
    }
}
