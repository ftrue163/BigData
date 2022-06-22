package com.atguigu.flinkcdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_SQL {
    public static void main(String[] args) throws Exception {
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //SQL 创建一张表映射到Flink MySQL CDC
        tableEnv.executeSql("create table mysql_binlog (" +
                "    id int not null," +
                "    tm_name string," +
                "    logo_url string" +
                ") with (" +
                "    'connector' = 'mysql-cdc'," +
                "    'hostname' = 'hadoop102'," +
                "    'port' = '3306'," +
                "    'username' = 'root'," +
                "    'password' = '123456zzQ!?#'," +
                "    'database-name' = 'gmall_flink_0625'," +
                "    'table-name' = 'base_trademark'" +
                ")");

        tableEnv.executeSql("select * from mysql_binlog").print();
    }
}
