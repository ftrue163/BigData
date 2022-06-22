package com.atguigu.flinksql.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink01_HiveCatalog {
    public static void main(String[] args) {
        //设置访问的用户
        //设置用户权限
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String name            = "myhive";  // Catalog 名字
        String defaultDatabase = "default"; // 默认数据库
        String hiveConfDir     = "flink/conf"; // hive配置文件的目录. 需要把hive-site.xml添加到该目录


        //3.创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);

        //4.注册HiveCataLog
        tableEnv.registerCatalog(name, hiveCatalog);

        //设置相关参数
        tableEnv.useCatalog(name);
        tableEnv.useDatabase(defaultDatabase);

        //指定SQL语法为Hive语法
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("select * from emp").print();
    }
}
