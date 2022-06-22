package com.atguigu.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 创建连接    按照官方要求的标准使用单例设计模式创建
 */
public class HBaseUtilConnection {
    //声明静态属性
    public static Connection connection;

    //初始化单例连接
    static {
        try {
            //1.获取配置类
            Configuration conf = HBaseConfiguration.create();
            //2.给配置类添加配置
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            //3.创建连接
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

    public static void main(String[] args) {
        System.out.println(connection);
    }

}
