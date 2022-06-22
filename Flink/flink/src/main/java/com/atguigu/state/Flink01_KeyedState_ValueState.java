package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * KeyedState_ValueState
 * 检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
 */
public class Flink01_KeyedState_ValueState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为JavaBean类型的数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });


        //分组
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorDStream.keyBy(WaterSensor::getId);

        //使用 KeyedStream 的 process 方法。使用 ValueState。
        waterSensorStringKeyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //定义一个值键控状态类型用来保存上一次的水位
            private ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                //从状态中获取上一次水位值
                Integer lastVc = valueState.value() == null ? 0 : valueState.value();

                //拿当前水位和上一次水位对比
                if (Math.abs(value.getVc() - lastVc) > 10) {
                    out.collect("水位超过10");
                }

                //更新状态
                //将当前水位保存到状态中
                valueState.update(value.getVc());
            }
        }).print();

        env.execute();
    }
}
