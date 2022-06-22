package com.sunjincheng.no31_e2e_exactlyonce;


import com.sunjincheng.no31_e2e_exactlyonce.functions.MapFunctionWithException;
import com.sunjincheng.no31_e2e_exactlyonce.functions.MapFunctionWithException_My;
import com.sunjincheng.no31_e2e_exactlyonce.functions.ParallelCheckpointedSource;
import com.sunjincheng.no31_e2e_exactlyonce.functions.ParallelCheckpointedSource_My;
import com.sunjincheng.no31_e2e_exactlyonce.functions.StateProcessFunction;
import com.sunjincheng.no31_e2e_exactlyonce.functions.Tuple3KeySelector;
import com.sunjincheng.no31_e2e_exactlyonce.functions.Tuple3KeySelector_My;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.concurrent.TimeUnit;

/**
 * 项目名称：流计算容错语义的代码示例
 * 功能描述：本测试核心是演示流计算语义 at-most-once, at-least-once, exactly-once, e2e-exactly-once
 * 操作步骤：
 *      1. 直接运行程序，观察at-most-once语义效果
 *      2. 打开atLeastOnce(env)方法，观察at-least-once的效果，主要是要和exactlyOnce(env)进行输出对比
 *      3. 打开exactlyOnce(env)方法，观察exactly-once的效果，主要是要和atLeastOnce(env)进行输出对比
 *      4. 打开exactlyOnce2(env)方法，观察print效果(相当于sink)，主要是要和exactlyOnce(env)进行输出对比
 *      5. 打开e2eExactlyOnce(env)方法，观察print效果(相当于sink)，主要是要和exactly-once进行输出对比
 *
 * 作者：张智奇
 * 日期：2022-01-15
 */
public class E2eExactlyOnceTestCase_My {
    public static void main(String[] args) throws Exception {
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启checkpoint
        env.enableCheckpointing(1000L);
        //设置restart策略
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(1, TimeUnit.MILLISECONDS)));

        //atMostOnce(env);
        //atLeastOnce(env);
        //atExactlyOnce(env);
        atExactlyOnce2(env);


        env.execute("E2e-Exactly-Once");
    }

    //即使设置了EXACTLY_ONCE，打印时数据还是出现了重复的现象
    private static void atExactlyOnce2(StreamExecutionEnvironment env) {
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        baseLogic(env).print();
    }


    //atLeastOnce方法和atExactlyOnce方法只有CheckpointingMode不一样，其它的都一样
    private static void atLeastOnce(StreamExecutionEnvironment env) {
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        baseLogic(env).process(new StateProcessFunction()).print();
    }

    private static void atExactlyOnce(StreamExecutionEnvironment env) {
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        baseLogic(env).process(new StateProcessFunction()).print();
    }

    private static KeyedStream<Tuple3<String, Long, String>, String> baseLogic(StreamExecutionEnvironment env) {
        DataStreamSource<Tuple3<String, Long, String>> source1 = env.addSource(new ParallelCheckpointedSource_My("S1"));
        DataStreamSource<Tuple3<String, Long, String>> source2 = env.addSource(new ParallelCheckpointedSource_My("S2"));

        //为什么一个map为10，另一个map为200？
        //一个map消费的快，一个map消费的快，当进行checkpoint时，消费快的map（在CheckpointingMode.AT_LEAST_ONCE下）会更快的收集到barrier，从而会继续消费数据，当任务出现故障并恢复时，就会出现at-least-once的现象
        SingleOutputStreamOperator<Tuple3<String, Long, String>> ds1 = source1.map(new MapFunctionWithException_My(10L));
        SingleOutputStreamOperator<Tuple3<String, Long, String>> ds2 = source2.map(new MapFunctionWithException_My(200L));

        return ds1.union(ds2).keyBy(new Tuple3KeySelector_My());
    }

    /**
     * 模拟无状态的数据源，同时数据是根据时间的推移而产生的，所以一旦流计算过程发生异常，那么异常期间的数据就丢失了，也就是at-most-once
     * @param env
     */
    private static void atMostOnce(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Tuple2<String, Long>> source = env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                while (true) {
                    ctx.collect(new Tuple2<>("key", System.currentTimeMillis()));
                    //每一毫秒生成一条数据
                    Thread.sleep(1);
                }
            }

            @Override
            public void cancel() {

            }
        });

        source.map(new MapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                if (value.f1 % 10 == 0) {
                    String msg = String.format("Bad data [%d]...", value.f1);
                    throw new RuntimeException(msg);
                }
                return value;
            }
        }).print();
    }
}
