package com.atguig.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * 1.百度
 * 对于所有客户端代码
 * 国际通用步骤
 * 1.创建客户端对象
 * 2.使用客户端对象进行一些操作
 * 3.关闭客户端对象
 * 1.jdbc 2.hdfs 3.zookeeper 4.kafka
 */
public class Hdfs_Demo {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        // URI uri = new URI("hdfs://hadoop102:8020");
        URI uri = URI.create("hdfs://hadoop102:8020");
        //创建一个配置对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(uri, conf, "atguigu");
        System.out.println(fs.getClass().getName()); //org.apache.hadoop.hdfs.DistributedFileSystem
        FileSystem fs2 = FileSystem.get(conf);
        System.out.println(fs2.getClass().getName()); //org.apache.hadoop.fs.LocalFileSystem

        //创建目录
        fs.mkdirs(new Path("/java"));

        //关闭资源
        fs.close();
    }
}
