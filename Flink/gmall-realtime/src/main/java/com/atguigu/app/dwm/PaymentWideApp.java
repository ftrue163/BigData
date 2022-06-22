package com.atguigu.app.dwm;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

//数据流：web/app -> nginx -> 业务服务器 -> Mysql -> FlinkApp -> Kafka(ODS) -> FlinkApp -> Kafka/HBase(DWD) -> FlinkApp(Redis) -> Kafka(DWM) -> FlinkApp -> Kafka(DWM)
//程  序：Mock -> Mysql -> FlinkCDC -> Kafka(ZK) -> BaseDbApp -> Kafka/HBase(ZK/HDFS) -> OrderWideApp -> Kafka -> PaymentWideApp -> Kafka
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境设置跟Kafka的分区数保持一致

        //设置状态后端
        //env.setStateBackend(new FsStateBackend(""));
        //开启CK
        //env.enableCheckpointing(5000); //生产环境设置分钟级
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setCheckpointTimeout(10000);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //TODO 2.读取Kafka数据创建流
        String groupId = "payment_wide_group_210625";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        DataStreamSource<String> orderWideStrDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentStrDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));

        //TODO 3.将每行数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            return recordTimestamp;
                        }
                    }
                }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentStrDS.map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        try {
                            return sdf.parse(element.getCreate_time()).getTime();
                        } catch (ParseException e) {
                            return recordTimestamp;
                        }
                    }
                }));

        //TODO 4.实现双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = orderWideDS.keyBy(OrderWide::getOrder_id)
                .intervalJoin(paymentInfoDS.keyBy(PaymentInfo::getOrder_id))
                .between(Time.seconds(-5L), Time.minutes(15L))
                .process(new ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>() {
                    @Override
                    public void processElement(OrderWide orderWide, PaymentInfo paymentInfo, ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //TODO 5.将数据写出到Kafka
        //note：paymentWideDS>>>>>> 的个数小于 orderDetail的 个数，因为有的订单没有支付
        //可以去MySQL中写sql进行查询来验证测试结果的正确性
        paymentWideDS.print("paymentWideDS>>>>>>");
        paymentWideDS.map(JSONObject::toJSONString).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        //TODO 6.启动任务
        env.execute("PaymentWideApp");
    }
}
