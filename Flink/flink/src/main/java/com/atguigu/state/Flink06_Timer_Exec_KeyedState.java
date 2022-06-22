package com.atguigu.state;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 监控水位传感器的水位值，如果水位值在五秒钟之内(processing time)连续上升，则报警
 */
public class Flink06_Timer_Exec_KeyedState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
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

        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //声明一个变量用来保存上一次的水位
            private ValueState<Integer> lastVc;
            //声明一个变量用来保存定时器的时间
            private ValueState<Long> timer;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态变量
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVc", Integer.class, Integer.MIN_VALUE));
                timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
            }

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                //判断水位是否上升
                if (value.getVc() > lastVc.value()) {
                    //水位上升
                    //注册定时器 (注册过的话跳过)
                    if (timer.value() == null) {
                        timer.update(System.currentTimeMillis() + 5000);
                        System.out.println("注册定时器：" + ctx.getCurrentKey() + timer.value());
                        ctx.timerService().registerProcessingTimeTimer(timer.value());
                    }
                } else {
                    //如果水位没有上升则删除定时器
                    System.out.println("删除定时器：" + ctx.getCurrentKey() + timer.value());
                    ctx.timerService().deleteProcessingTimeTimer(timer.value());
                    //重置定时器
                    timer.clear();
                }

                //将水位更新
                lastVc.update(value.getVc());
                out.collect(value.toString());
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Tuple, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                //将报警信息放到侧输出流中
                ctx.output(new OutputTag<String>("output") {
                }, ctx.getCurrentKey() + "报警！！！ 水位连续5s上升");
                //重置定时器时间
                timer.clear();
            }
        });


        result.print("主流");

        DataStream<String> sideOutput = result.getSideOutput(new OutputTag<String>("output") {
        });
        sideOutput.print("侧流");

        env.execute();
    }
}
