package com.sunjincheng.no24_restart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 项目名称：Apache Flink 知其然，知其所以然 - restart
 * 功能描述：演示没有设置重启策略时候，Flink作业异常后的行为 - 直接推出。
 * 操作步骤：
 *      1. 接运行程序，当作业打印出99之后，作业退出。
 *      2. 增加env.setRestartStrategy(RestartStrategies.noRestart());观察行为和默认一样。
 * <p>
 * 作者：张智奇
 * 日期：2021/01/08
 */
public class NoRestartJob {
    public static void main(String[] args) throws Exception {
        Logger log = LoggerFactory.getLogger(NoRestartJob.class);

        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置重启策略
        env.setRestartStrategy(RestartStrategies.noRestart());

        //添加Source
        DataStreamSource<Tuple3<String, Integer, Long>> source = env.addSource(new SourceFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
                int index = 1;
                while (true) {
                    ctx.collect(new Tuple3<>("key", index++, System.currentTimeMillis()));
                    //Just for test
                    Thread.sleep(100);
                }
            }

            @Override
            public void cancel() {

            }
        });

        source.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple3<String, Integer, Long> value) throws Exception {
                if (value.f1 % 100 == 0) {
                    String msg = String.format("Bad data [%d] ...", value.f1);
                    log.error(msg);
                    //抛出异常，作业根据 配置 的重启策略进行恢复，无重启策略作业直接退出
                    throw new RuntimeException(msg);
                }

                return new Tuple2<>(value.f0, value.f1);
            }
        }).print();

        //开始执行
        env.execute("NoRestart");
    }
}
