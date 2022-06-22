package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


//数据流：web/app -> Nginx -> 日志服务器 -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：Mock    -> Nginx -> logger.sh -> Kafka(ZK)  -> BaseLogApp -> Kafka(ZK)
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境设置跟Kafka的分区数保持一致

        /*//设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/gmall/ck"));
        //开启CK
        env.enableCheckpointing(5000L);  //生产环境设置分钟级
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);*/


        //TODO 2.消费Kafka ods_base_log 主题数据
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_log_app_210625"));

        //TODO 3.转换流中的String类型数据为JSONObject类型数据    过滤脏数据（不符合JSON格式的数据）   使用侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                //判断数据是否符合JSON格式
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(outputTag, value);
                }
            }
        });
        //提取侧输出流中的数据
        DataStream<String> sideOutput = jsonObjDS.getSideOutput(outputTag);
        sideOutput.print("Dirty>>>>>>");

        //TODO 4.按照Mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 5.新老用户校验   状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String isNew = value.getJSONObject("common").getString("is_new");

                if ("1".equals(isNew)) {

                    String state = valueState.value();

                    if (state != null) {
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        valueState.update("0");
                    }
                }

                return value;
            }
        });

        //TODO 6.分流   使用侧输出流   页面流、曝光流、启动流
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };
        OutputTag<String> startOutputTag = new OutputTag<String>("page") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                //获取启动日志
                if (value.getString("start") != null) {
                    //将启动日志数据 写入启动日志侧输出流
                    ctx.output(startOutputTag, value.toJSONString());
                } else {
                    //非启动日志 即为页面日志  将数据写入主流
                    out.collect(value.toJSONString());

                    //获取曝光数据
                    JSONArray jsonArray = value.getJSONArray("displays");
                    if (jsonArray != null && jsonArray.size() > 0) {
                        //提取页面ID
                        String pageId = value.getJSONObject("page").getString("page_id");

                        //遍历写出
                        for (int i = 0; i < jsonArray.size(); i++) {
                            JSONObject display = jsonArray.getJSONObject(i);
                            display.put("page_id", pageId);

                            //将曝光日志数据 写入曝光侧输出流
                            ctx.output(displayOutputTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 7.将3个流写入到对应的Kafka主题
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutputTag);
        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);

        pageDS.print("Page>>>>>>");
        displayDS.print("Display>>>>>>");
        startDS.print("Start>>>>>");

        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));

        //TODO 8.启动任务
        env.execute("BaseLogApp");
    }
}
