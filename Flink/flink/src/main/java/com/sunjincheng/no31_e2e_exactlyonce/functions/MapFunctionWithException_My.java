package com.sunjincheng.no31_e2e_exactlyonce.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

/**
 * 触发异常
 */
public class MapFunctionWithException_My extends RichMapFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>> implements CheckpointListener {
    private long delay;
    private transient volatile boolean needFail = false;

    public MapFunctionWithException_My(long delay) {
        this.delay = delay;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public Tuple3<String, Long, String> map(Tuple3<String, Long, String> value) throws Exception {
        //线程睡眠的作用是用来模拟map消费速度的快慢
        //本次测试的场景会让一个map消费的快，一个map消费的慢
        Thread.sleep(delay);
        if (needFail) {
            throw new RuntimeException("Error for testing...");
        }
        return value;
    }

    //当checkpoint完成后调用此方法
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        this.needFail = true;
        System.err.println(String.format("MAP - CP SUCCESS [%d]", checkpointId));
    }
}
