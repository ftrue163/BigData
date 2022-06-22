package com.atguigu.app.fun;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private OutputTag<JSONObject> outputTag;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor, OutputTag<JSONObject> outputTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //读取MySQL配置表放入内存【Map】中！
    }


    //任务：1.过滤所需字段   2.分流（kafka或hbase）
    //value:{"database":"", "tableName":"base_trademark", "before":{}, "after":{"id":"", "tm_name":"", "logo_url":""....}, "type":""}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //1.根据主键读取广播状态对应的数据
        ReadOnlyBroadcastState<String, TableProcess> readOnlyBroadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = readOnlyBroadcastState.get(key);

        if (tableProcess != null) {
            //2.根据广播状态数据  过滤所需字段
            filterColumns(value.getJSONObject("after"), tableProcess.getSinkColumns());

            //给JSON数据增加sinkTable字段，方便后续把数据分别写入对应Kafka主题或者HBase表中
            value.put("sinkTable", tableProcess.getSinkTable());

            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                ctx.output(outputTag, value);
            }
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                out.collect(value);
            }

        } else {
            //从内存的Map中尝试获取数据
            System.out.println(key + "不存在！");
        }
    }

    //任务：1.将广播流中的数据（TableProcess表的数据）保存到MapState中  2.检查HBase表是否存在，如果不存在则建表
    //value:{"database":"", "tableName":"table_process", "before":{}, "after":{"source_table":""....}, "type":""}
    //table_process表字段：source_table  operate_type  sink_type sink_table  sink_columns sink_pk  sink_extend
    //key：source_table + operate_type
    //value: TableProcess(封装after内的数据)
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //1.解析数据为JavaBean  即为TableProcess
        JSONObject jsonObject = JSONObject.parseObject(value);
        TableProcess tableProcess = JSONObject.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.检查HBase表是否存在，如果不存在则建表
        String sinkType = tableProcess.getSinkType();
        if ("hbase".equals(sinkType)) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPK(),
                    tableProcess.getSinkExtend());
        }

        //3.将数据写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }



    //根据广播状态中sink_columns字段的数据  过滤所需字段
    //after:{"id":"","tm_name":"","logo_url":"","name":""}  sinkColumns:id,tm_name
    //{"id":"","tm_name":""}
    private void filterColumns(JSONObject after, String sink_columns) {
        String[] columns = sink_columns.split(",");
        List<String> columnList = Arrays.asList(columns);

        //方式1
        /*Set<Map.Entry<String, Object>> entries = after.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!columnList.contains(next.getKey())) {
                iterator.remove();
            }
        }*/

        //方式2
        Set<Map.Entry<String, Object>> entries = after.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));


        //错误的写法：java.util.ConcurrentModificationException
        /*for (String key : after.keySet()) {
            if (!columnList.contains(key)) {
                after.remove(key);
            }
        }*/
    }


    //检查HBase表是否存在，如果不存在则建表
    //create table if not exists db.tn (id varchar primary key, tm_name varchar) sinkExtend
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            if (sinkPk == null || sinkPk.equals("")) {
                sinkPk = "id";
            }

            //1.封装建表sql语句
            StringBuilder stringBuilder = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append(" (");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                //取出列名
                String column = columns[i];
                stringBuilder.append(column).append(" varchar");

                //判断是否为主键
                if (column.equals(sinkPk)) {
                    stringBuilder.append(" primary key");
                }

                //判断当前如果不是最后一个字段，则添加“,”
                if (i < columns.length - 1) {
                    stringBuilder.append(", ");
                }
            }

            stringBuilder.append(") ").append(sinkExtend);
            String createTableSQL = stringBuilder.toString();
            //测试 打印建表sql语句
            System.out.println(createTableSQL);

            //2.通过Phoenix连接  执行建表sql语句
            preparedStatement = connection.prepareStatement(createTableSQL);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表：" + sinkTable + " 失败");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
