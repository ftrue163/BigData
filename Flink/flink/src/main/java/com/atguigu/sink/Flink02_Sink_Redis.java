package com.atguigu.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });
        SingleOutputStreamOperator<String> jsonStrDStream = waterSensorDStream.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return JSONObject.toJSONString(value);
            }
        });

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();

        //将数据写入Redis中
        jsonStrDStream.addSink(new RedisSink<>(conf, new MyRedisMapper()));

        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<String> {
        /**
         * additionalKey：指的是在使用Hash类型时，redis的Key
         * @return
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "0625");
            //return new RedisCommandDescription(RedisCommand.SET);
        }

        /**
         * 在使用Hash类型时，这个方法指定的key为Hash的field
         * @param data
         * @return
         */
        @Override
        public String getKeyFromData(String data) {
            JSONObject jsonObject = JSONObject.parseObject(data);
            return jsonObject.getString("id");
        }

        /**
         * 指定存入的数据
         * @param data
         * @return
         */
        @Override
        public String getValueFromData(String data) {
            return data;
        }
    }
}
