package com.atguigu.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    public static JedisPool jedisPool = null;

    public static Jedis getJedis() {
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);  //最大可用连接数
            jedisPoolConfig.setBlockWhenExhausted(true);  //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(6000L);  //最大等待时间
            jedisPoolConfig.setMaxIdle(10);  //最大闲置连接数
            jedisPoolConfig.setMinIdle(10);  //最小闲置连接数
            jedisPoolConfig.setTestOnBorrow(true);  //取连接水位时候进行一下测试 ping pong

            jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379, 1000);

            System.out.println("开辟连接池");
        }

        //System.out.println("连接池：" + jedisPool.getNumActive());
        return jedisPool.getResource();
    }

}
