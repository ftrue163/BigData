package com.atguigu.flinksql.udf;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink02_Fun_UDTF {
    public static void main(String[] args) {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //3.创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.将流转为表
        Table table = tableEnv.fromDataStream(waterSensorDStream);

        //使用自定义函数的两种方式
        //方式1：不注册直接使用
        /*table.joinLateral(call(MyUDTF.class, $("id")))
                .select($("id"), $("word"))
                .execute()
                .print();*/


        //方式2：先注册再使用
        //注册函数
        tableEnv.createTemporaryFunction("myUDTF", MyUDTF.class);

        /*table.leftOuterJoinLateral(call("myUDTF", $("id")))
                .select($("id"), $("word"))
                .execute()
                .print();*/
        tableEnv.executeSql("select id, word from " + table + ", LATERAL TABLE(MyUDTF(id))").print();
        /*tableEnv.executeSql("select id, word from " + table + " left join lateral table(myUDTF(id)) on true")
                .print();*/
    }


    //自定义一个类实现UDTF(表函数TableFunction)函数 根据id按照空格切分获取到字母以及数字
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class MyUDTF extends TableFunction<Row> {
        public void eval(String value) {
            String[] split = value.split("_");
            for (String s : split) {
                collect(Row.of(s));
            }
        }
    }
}
