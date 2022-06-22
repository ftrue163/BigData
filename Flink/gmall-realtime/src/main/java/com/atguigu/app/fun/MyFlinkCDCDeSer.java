package com.atguigu.app.fun;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyFlinkCDCDeSer implements DebeziumDeserializationSchema<String> {
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
