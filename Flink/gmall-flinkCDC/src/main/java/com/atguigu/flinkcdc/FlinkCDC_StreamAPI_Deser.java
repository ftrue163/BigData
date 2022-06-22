package com.atguigu.flinkcdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class FlinkCDC_StreamAPI_Deser {
    public static void main(String[] args) throws Exception {
        //创建流执行执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //创建Flink CDC MySQL 的 SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456zzQ!?#")
                .databaseList("gmall_flink_0625")
                //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
                .tableList("gmall_flink_0625.base_trademark")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeser())
                .build();

        //获取流数据并打印
        env.addSource(sourceFunction).print();

        env.execute();
    }


    //自定义Flink CDC MySQL的反序列化器   在SourceRecord数据类型中提取所需的数据并封装为String类型

    /**
     * 分析后，所需的数据内容和格式如下：
     * {
     *     "database": "gmall_flink_0625",
     *     "tableName": "aaa",
     *     "after": {"id": "123", "name", "zzk", ......},
     *     "before": {"id": "123", "name", "zzk2B", ......},
     *     "type": "insert"
     * }
     */
    private static class MyDeser implements DebeziumDeserializationSchema<String> {
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //创建一个JSONObject用来存放最终封装的数据
            JSONObject jsonObject = new JSONObject();

            //提取数据库名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String database = split[1];

            //提取表名
            String tableName = split[2];

            Struct value = (Struct) sourceRecord.value();
            //获取after数据
            Struct afterStruct = value.getStruct("after");
            JSONObject afterJson = new JSONObject();
            //判断是否有after
            if (afterStruct != null) {
                for (Field field : afterStruct.schema().fields()) {
                    afterJson.put(field.name(), afterStruct.get(field));
                }

            }

            //获取before数据
            Struct beforeStruct = value.getStruct("before");
            JSONObject beforeJson = new JSONObject();
            //判断是否有before
            if (beforeStruct != null) {
                for (Field field : beforeStruct.schema().fields()) {
                    beforeJson.put(field.name(), beforeStruct.get(field));
                }

            }

            //获得操作类型 DELETE UPDATE CREATE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            String type = operation.toString().toLowerCase();
            if ("create".equals(type)) {
                type = "insert";
            }

            //封装数据到JSONObject
            jsonObject.put("database", database);
            jsonObject.put("tableName", tableName);
            jsonObject.put("after", afterJson);
            jsonObject.put("before", beforeJson);
            jsonObject.put("type", type);

            collector.collect(jsonObject.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}

//CREATE
/*
SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={ts_sec=1637651592, file=mysql-bin.000011, pos=1158, row=1, server_id=1, event=2}}
ConnectRecord{
    topic='mysql_binlog_source.gmall_flink_0625.base_trademark',
    kafkaPartition=null,
    key=Struct{id=13},
    keySchema=Schema{mysql_binlog_source.gmall_flink_0625.base_trademark.Key:STRUCT},
    value=Struct{
        after=Struct{id=13,tm_name=zzk,logo_url=5b},
        source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1637651592000,db=gmall_flink_0625,table=base_trademark,server_id=1,file=mysql-bin.000011,pos=1317,row=0,thread=10},
        op=c,
        ts_ms=1637651592322},
    valueSchema=Schema{mysql_binlog_source.gmall_flink_0625.base_trademark.Envelope:STRUCT},
    timestamp=null,
    headers=ConnectHeaders(headers=)}
*/

//UPDATE
/*
SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={ts_sec=1637652153, file=mysql-bin.000011, pos=1466, row=1, server_id=1, event=2}}
ConnectRecord{
    topic='mysql_binlog_source.gmall_flink_0625.base_trademark',
    kafkaPartition=null,
    key=Struct{id=13},
    keySchema=Schema{mysql_binlog_source.gmall_flink_0625.base_trademark.Key:STRUCT},
    value=Struct{
        before=Struct{id=13,tm_name=zzk,logo_url=5b},
        after=Struct{id=13,tm_name=zzk,logo_url=6b},
        source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1637652153000,db=gmall_flink_0625,table=base_trademark,server_id=1,file=mysql-bin.000011,pos=1625,row=0,thread=11},
        op=u,
        ts_ms=1637652153366},
    valueSchema=Schema{mysql_binlog_source.gmall_flink_0625.base_trademark.Envelope:STRUCT},
    timestamp=null,
    headers=ConnectHeaders(headers=)}
*/

//DELETE
/*
SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={ts_sec=1637652549, file=mysql-bin.000011, pos=1793, row=1, server_id=1, event=2}}
ConnectRecord{
    topic='mysql_binlog_source.gmall_flink_0625.base_trademark',
    kafkaPartition=null,
    key=Struct{id=13},
    keySchema=Schema{mysql_binlog_source.gmall_flink_0625.base_trademark.Key:STRUCT},
    value=Struct{
        before=Struct{id=13,tm_name=zzk,logo_url=6b},
        source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1637652549000,db=gmall_flink_0625,table=base_trademark,server_id=1,file=mysql-bin.000011,pos=1952,row=0,thread=12},
        op=d,
        ts_ms=1637652549970},
    valueSchema=Schema{mysql_binlog_source.gmall_flink_0625.base_trademark.Envelope:STRUCT},
    timestamp=null,
    headers=ConnectHeaders(headers=)}
*/
