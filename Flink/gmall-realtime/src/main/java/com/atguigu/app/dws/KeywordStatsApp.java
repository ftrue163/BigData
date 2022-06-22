package com.atguigu.app.dws;

import com.atguigu.app.fun.SplitFunction;
import com.atguigu.bean.KeywordStats;
import com.atguigu.utils.ClickHouseUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//数据流：web/app -> Nginx -> 日志服务器 -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse
//程  序：Mock    -> Nginx -> Logger.sh -> Kafka(ZK) -> BaseLogApp -> Kafka -> KeywordStatsApp -> ClickHouse
public class KeywordStatsApp {
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

        //TODO 2. 使用DDL方式创建动态表
        String groupId = "keyword_stats_app_210625";
        String pageViewSourceTopic = "dwd_page_log";

        tableEnv.executeSql("create table page_log ( " +
                "   page map<string, string>, " +
                "   ts bigint, " +
                "   rowtime as to_timestamp(from_unixtime(ts / 1000)), " +
                "   watermark for rowtime as rowtime - interval '1' second " +
                ") with ( " +
                "   'connector' = 'kafka', " +
                "   'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "   'properties.group.id' = 'keyword_stats_app_210625', " +
                "   'topic' = 'dwd_page_log', " +
                "   'scan.startup.mode' = 'group-offsets', " +
                "   'format' = 'json' " +
                ")");

        //TODO 3. 过滤数据，只需要搜索的日志
        Table filterTable = tableEnv.sqlQuery("select  " +
                "   page['item'] key_word, " +
                "   rowtime " +
                "from page_log " +
                "where page['item_type'] = 'keyword' and page['item'] is not null");

        //TODO 4. 使用UDTF进行分词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        tableEnv.createTemporaryView("filter_table", filterTable);
        Table wordTable = tableEnv.sqlQuery("select " +
                "   word, " +
                "   rowtime " +
                "from filter_table, " +
                "lateral table(SplitFunction(key_word))");
        /*Table wordTable = tableEnv.sqlQuery("select " +
                "   word, " +
                "   rowtime " +
                "from " + filterTable + ", " +
                "lateral table(SplitFunction(key_word))");*/

        //TODO 5. 分组开窗聚合
        tableEnv.createTemporaryView("word_table", wordTable);
        Table resultTable = tableEnv.sqlQuery("select " +
                "   'source' source, " +
                "   date_format(tumble_start(rowtime, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt, " +
                "   date_format(tumble_end(rowtime, interval '10' second), 'yyyy-MM-dd HH:mm:ss') edt, " +
                "   word keyword, " +
                "   count(*) ct, " +
                "   unix_timestamp() * 1000 ts " +
                "from word_table " +
                "group by  " +
                "   word, " +
                "   tumble(rowtime, interval '10' second)");

        //TODO 6. 将动态表转换为流
        //resultTable表的字段名称以及个数 和 KeywordStats类需要一样   但顺序没要求
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        //TODO 7. 将数据写出到ClickHouse
        keywordStatsDataStream.print("keywordStatsDataStream>>>>>>");
        keywordStatsDataStream.addSink(ClickHouseUtil.getJdbcSink("insert into keyword_stats_210625 (keyword, ct, source, stt, edt, ts)  values (?, ?, ?, ?, ?, ?)"));

        //TODO 8. 启动任务
        env.execute("KeywordStatsApp");
    }
}
