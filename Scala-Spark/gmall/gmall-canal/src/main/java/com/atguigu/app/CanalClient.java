package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.获取canal连接对象
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");


        while (true) {
            //2.获取连接
            canalConnector.connect();

            //3.选择订阅的数据库
            canalConnector.subscribe("gmall21.*");

            //4.获取Message(多个sql执行结果)
            Message message = canalConnector.get(100);

            //5.
            List<CanalEntry.Entry> entries = message.getEntries();

            if (entries.size() <= 0) {
                try {
                    Thread.sleep(5000);
                    System.out.println("没有数据休息一会。。。");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : entries) {
                    //获取表名
                    String tableName = entry.getHeader().getTableName();

                    //根据entry类型获取序列化数据
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        //获取序列化数据
                        ByteString storeValue = entry.getStoreValue();

                        //对数据做反序列化操作
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //根据不同的需求获取不同表的数据
                        handle(tableName, eventType, rowDatasList);
                    }
                }
            }
        }
    }

    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //获取每一行数据
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();
                //获取每一行的每一列数据
                for (CanalEntry.Column column : columnsList) {
                    jsonObject.put(column.getName(), column.getValue());
                }
                System.out.println(jsonObject.toJSONString());

                //将数据发送至Kafka
                MyKafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER, jsonObject.toJSONString());
            }
        }
    }


}



















