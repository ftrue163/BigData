package com.atguigu.flinksql.udf;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink01_Fun_UDF {
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
        /*table.select($("id"), call(MyUDF.class, $("id")))
                .execute()
                .print();*/


        //方式2：先注册再使用
        //注册函数
        tableEnv.createTemporaryFunction("strLen", MyUDF.class);

        /*table.select($("id"), call("strLen", $("id")))
                .execute()
                .print();*/
        tableEnv.executeSql("select id, strLen(id) from " + table)
                .print();
    }


    //自定义一个类实现UDF(标量函数ScalarFunction)函数 根据id获取id的字符串长度
    public static class MyUDF extends ScalarFunction {
        public Integer eval(String id) {
            return id.length();
        }
    }
}
