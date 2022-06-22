package com.atguigu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 通过代码的方式进行 Pre-Splitting Regions
 * 但在生产中一般不适用这种方式
 */
public class HBasePreCreateRegions {
    public static void main(String[] args) throws IOException {
        // 1.获取配置类
        Configuration conf = HBaseConfiguration.create();

        // 2.给配置类添加配置
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        // 3.获取连接
        Connection connection = ConnectionFactory.createConnection(conf);

        // 4.获取admin
        Admin admin = connection.getAdmin();

        // 5.获取descriptor的builder
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf("bigdata", "staff4"));

        // 6. 添加列族
        builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build());

        // 7.创建对应的切分
        byte[][] splits = new byte[3][];
        splits[0] = Bytes.toBytes("aaa");
        splits[1] = Bytes.toBytes("bbb");
        splits[2] = Bytes.toBytes("ccc");

        // 8.创建表
        admin.createTable(builder.build(), splits);

        // 9.关闭资源
        admin.close();
        connection.close();
    }

}
