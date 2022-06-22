package com.atguigu.state;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * KeyedState_ListState
 * 针对每个传感器输出最高的3个水位值
 */
public class Flink02_KeyedState_ListState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据并转为JavaBean
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
                    //定义一个list键控状态来保存三个最高的水位
                    private ListState<Integer> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //先将当前的水位存到状态中
                        listState.add(value.getVc());

                        //取出状态中的数据并排序
                        ArrayList<Integer> vcs = new ArrayList<>();
                        for (Integer integer : listState.get()) {
                            vcs.add(integer);
                        }

                        //对list集合中的数据做排序，倒序排序
                        vcs.sort((o1, o2) -> o2 - o1);

                        //删除最小的数据
                        if (vcs.size() > 3) {
                            vcs.remove(3);
                        }

                        //将list集合中的数据更新到list状态中
                        listState.update(vcs);

                        out.collect(vcs.toString());
                    }
                }).print();

        env.execute();
    }
}
