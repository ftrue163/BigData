package com.atguigu.app.dws;

import com.atguigu.bean.ProvinceStats;
import com.atguigu.utils.ClickHouseUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//数据流：web/app -> nginx -> 业务服务器 -> Mysql -> FlinkApp -> Kafka(ODS) -> FlinkApp -> Kafka/HBase(DWD) -> FlinkApp(Redis) -> Kafka(DWM) -> FlinkApp -> ClickHouse
//程  序：Mock -> Mysql -> FlinkCDC -> Kafka(ZK) -> BaseDbApp -> Kafka/HBase(ZK/HDFS) -> OrderWideApp(Redis) -> Kafka ->ProvinceStatsSqlApp -> ClickHouse
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境设置跟Kafka的分区数保持一致
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置状态后端
        //env.setStateBackend(new FsStateBackend(""));
        //开启CK
        //env.enableCheckpointing(5000); //生产环境设置分钟级
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setCheckpointTimeout(10000);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //TODO 2. 使用DDL方式创建动态表  提取事件时间并生成Watermark
        String groupId = "province_stats_210625";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("create table order_wide ( " +
                "   province_id bigint, " +
                "   province_name string, " +
                "   province_area_code string, " +
                "   province_iso_code string, " +
                "   province_3166_2_code string, " +
                "   order_id bigint, " +
                "   split_total_amount decimal, " +
                "   create_time string, " +
                "   rowtime as to_timestamp(create_time), " +
                "   watermark for rowtime as rowtime - interval '1' second " +
                ") with ( " +
                "   'connector' = 'kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "   'topic' = 'dwm_order_wide', " +
                "   'properties.group.id' = 'province_stats_sql_app_210625', " +
                "   'scan.startup.mode' = 'group-offsets', " +
                "   'format' = 'json' " +
                ")");

        //TODO 3. 分组开窗聚合  查询
        Table table = tableEnv.sqlQuery("select  " +
                "   date_format(tumble_start(rowtime, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt, " +
                "   date_format(tumble_end(rowtime, interval '10' second), 'yyyy-MM-dd HH:mm:ss') edt, " +
                "   province_id, " +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code, " +
                "   count(distinct order_id) order_count, " +
                "   sum(split_total_amount) order_amount, " +
                "   unix_timestamp() * 1000 ts " +
                "from order_wide " +
                "group by  " +
                "   province_id, " +
                "   province_name, " +
                "   province_area_code, " +
                "   province_iso_code, " +
                "   province_3166_2_code, " +
                "   tumble(rowtime, interval '10' second)");

        //TODO 4. 转换为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);

        //TODO 5. 将数据写出到ClickHouse
        provinceStatsDataStream.print("provinceStatsDataStream>>>>>>");
        provinceStatsDataStream.addSink(ClickHouseUtil.getJdbcSink("insert into province_stats_210625 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"));

        //TODO 6. 启动任务
        env.execute("ProvinceStatsSqlApp");
    }
}
