package com.sunjincheng.no31_e2e_exactlyonce.functions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class StateProcessFunction_My extends KeyedProcessFunction<String, Tuple3<String, Long, String>, Tuple3<String, Long, String>> {
    private transient ListState<Tuple3<String, Long, String>> processData;
    private final String STATE_NAME = "processData";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        processData = getRuntimeContext().getListState(new ListStateDescriptor<>(STATE_NAME, Types.TUPLE(Types.STRING, Types.LONG, Types.STRING)));
    }


    //如果有重复数据，则会打印出来
    //当有数据打印出来时，可以得出数据有被重复消费了，即at-least-once
    @Override
    public void processElement(Tuple3<String, Long, String> value, KeyedProcessFunction<String, Tuple3<String, Long, String>, Tuple3<String, Long, String>>.Context ctx, Collector<Tuple3<String, Long, String>> out) throws Exception {
        boolean isDuplicate = false;
        Iterator<Tuple3<String, Long, String>> it = processData.get().iterator();
        while (it.hasNext()) {
            if (it.next().equals(value)) {
                isDuplicate = true;
                break;
            }
        }

        if (isDuplicate) {
            //重复数据，进行收集
            out.collect(value);
        } else {
            //第一次消费的数据保存到状态中
            processData.add(value);
        }
    }
}
