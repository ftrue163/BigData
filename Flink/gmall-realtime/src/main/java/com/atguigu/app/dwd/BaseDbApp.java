package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.fun.DimSinkFunction;
import com.atguigu.app.fun.MyFlinkCDCDeSer;
import com.atguigu.app.fun.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;


//数据流：web/app -> Nginx -> 日志服务器 -> DB -> Flink CDC -> Kafka(ods_base_db) -> BaseDbApp -> kafka(dwd_)/hbase(dim_)
//程  序：Mock    -> DB -> Flink_CDCWithCustomerSchema  -> Kafka(ZK)  -> BaseLogApp -> Kafka(ZK) & HBase(Hadoop)
public class BaseDbApp {
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

        //TODO 2.读取kafka ods_base_db 主题创建主流
        DataStreamSource<String> dataStream = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db", "base_db_app_210625"));

        //TODO 3.将每行数据转换为JSONObject数据类型  过滤脏数据（不符合JSON格式的数据）
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(value);
                }
            }
        });

        //TODO 4.过滤删除数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(jsonObj -> !"delete".equals(jsonObj.getString("type")));

        //TODO 5.使用Flink CDC读取MySQL配置表并创建广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456zzQ!?#")
                .databaseList("gmall_210625_realtime")
                .tableList("gmall_210625_realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyFlinkCDCDeSer())
                .build();
        DataStreamSource<String> cdcDS = env.addSource(sourceFunction);
        //测试 打印
        //cdcDS.print("MySQL配置表>>>>>>");
        //value的数据类型选定为TableProcess   JSONObject也可以  但是没有TableProcess更好
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = cdcDS.broadcast(mapStateDescriptor);

        //TODO 6.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = filterDS.connect(broadcastStream);

        //TODO 7.根据配置流信息处理主流数据（分流）
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaDS = broadcastConnectedStream.process(new TableProcessFunction(mapStateDescriptor, hbaseTag));

        //TODO 8.将HBase数据写出
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);
        hbaseDS.print("HBase>>>>>>");
        hbaseDS.addSink(new DimSinkFunction());

        //TODO 9.将Kafka数据写出
        kafkaDS.print("Kafka>>>>>>");
        kafkaDS.addSink(MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
            //element:{"db":"","tableName":"","before":{},"after":{"id":""...},"type":"","sinkTable":""}
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sinkTable"), element.getString("after").getBytes());
            }
        }));

        //TODO 10.启动任务
        env.execute("BaseDbApp");
    }
}
