package com.atguigu.flinkcdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_StreamAPI {
    public static void main(String[] args) throws Exception {
        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /*//开启Checkpoint，每隔5秒做一次Checkpoint
        env.enableCheckpointing(5000L);

        //指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //指定从CK的重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));

        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));

        //设置访问hdfs的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/


        //创建Flink-MySQL-CDC的SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456zzQ!?#")
                .databaseList("gmall_flink_0625")
                //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的所有数据，注意：指定的时候需要使用"db.table"的方式
                .tableList("gmall_flink_0625.base_trademark")
                .startupOptions(StartupOptions.initial())
                //.startupOptions(StartupOptions.latest())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        //打印测试
        env.addSource(sourceFunction).print();

        env.execute();
    }
}
