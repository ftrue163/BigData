package com.atguigu.app.ods;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.fun.MyFlinkCDCDeSer;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 消费MySQl变化数据并将数据写入Kafka
 */
public class Flink_CDCWithCustomerSchema {
    public static void main(String[] args) throws Exception {
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //创建Flink CDC MySQL的SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456zzQ!?#")
                .databaseList("gmall_flink_0625")
                //.tableList("yourDatabaseName.yourTableName") // set captured table
                //.startupOptions(StartupOptions.initial())   //默认值为initial
                //可选值为 initial、earliest、latest
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyFlinkCDCDeSer())
                .build();

        //获取Flink CDC MySQL的流数据
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //将数据写入Kafka主题中
        streamSource.print();
        streamSource.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        env.execute();
    }
}
