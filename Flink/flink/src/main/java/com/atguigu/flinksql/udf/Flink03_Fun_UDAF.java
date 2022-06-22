package com.atguigu.flinksql.udf;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink03_Fun_UDAF {
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
        /*table
                .groupBy($("id"))
                .select($("id"), call(MyUDAF.class, $("vc")))
                .execute()
                .print();*/


        //方式2：先注册再使用
        //注册函数
        tableEnv.createTemporaryFunction("myUDAF", MyUDAF.class);

        /*table
                .groupBy($("id"))
                .select($("id"), call("myUDAF", $("vc")))
                .execute()
                .print();*/
        tableEnv.executeSql("select id, myUDAF(vc) from " + table + " group by id")
                .print();
    }


    //自定义一个类实现UDAF(表函数AggFunction)函数 根据vc求出平均值
    public static class MyUDAF extends AggregateFunction<Double, MyAcc> {
        @Override
        public Double getValue(MyAcc accumulator) {
            return accumulator.sum * 1.0 / accumulator.count;
        }

        @Override
        public MyAcc createAccumulator() {
            return new MyAcc();
        }

        public void accumulate(MyAcc accumulator, Integer value) {
            accumulator.sum += value;
            accumulator.count += 1;
        }
    }

    public static class MyAcc {
        public Integer sum = 0;
        public Integer count = 0;
    }
}
