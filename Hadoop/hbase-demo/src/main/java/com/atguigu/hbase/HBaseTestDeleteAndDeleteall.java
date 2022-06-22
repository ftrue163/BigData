package com.atguigu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseTestDeleteAndDeleteall {
    public static void main(String[] args) throws IOException {
        //1.获取配置类
        Configuration conf = HBaseConfiguration.create();

        //2.给配置类添加配置
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        //3.获取连接
        Connection connection = ConnectionFactory.createConnection(conf);

        //DDL操作对象

        //DML操作对象
        //connection.getTable()
    }
}
