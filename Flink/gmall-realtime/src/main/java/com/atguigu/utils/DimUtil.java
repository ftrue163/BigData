package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.phoenix.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * 根据key 获取HBase中的维度数据
 *
 * 优化：采用Redis缓存进行优化
 */
public class DimUtil {

    /**
     * 基本思路：先查询Redis，如果查不到，则去HBase中查询并将查询结果存入Redis中。
     *     小优化：不管何种方式查到数据，都给Redis中的key设置过期时间
     * @param connection  HBase的连接
     * @param table
     * @param key
     * @return
     */
    public static JSONObject getDimInfo(Connection connection, String table, String key) throws Exception {
        //查询Redis中的数据
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + table + ":" + key;
        String jsonStr = jedis.get(redisKey);
        if (jsonStr != null) {
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            //归还连接
            jedis.close();
            //返回结果数据
            return JSONObject.parseObject(jsonStr);
        }

        //构建查询Phoenix的sql语句
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + table + " where id = '" + key + "'";
        //System.out.println("QuerySql: " + querySql);

        //执行查询
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);

        //将数据写入Redis 并设置过期时间
        JSONObject dimInfo = queryList.get(0);
        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);

        //归还连接
        jedis.close();

        //返回结果
        return dimInfo;
    }


    //删除Redis中的数据
    public static void deleteDimInfo(String table, String key) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + table + ":" + key;

        jedis.del(redisKey);
        jedis.close();
    }

    /**
     * 测试 使用Redis缓存优化前后  速度对比
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        //第一次HBase会向zookeeper请求元数据  所以速度会慢一些
        //如果使用了Redis缓存优化  还会初始化JedisPool
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "41"));  //240
        long end = System.currentTimeMillis();
        //第二次及以后  因为元数据缓存在客户端了   所以会快一些
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "41"));  //10
        long end2 = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "41"));  //10
        long end3 = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "41"));  //10
        long end4 = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "41"));  //10
        long end5 = System.currentTimeMillis();

        System.out.println(end - start);
        System.out.println(end2 - end);
        System.out.println(end3 - end2);
        System.out.println(end4 - end3);
        System.out.println(end5 - end4);
        //没有使用Redis时的结果
        //876
        //212
        //305
        //204
        //170
        //使用Redis优化后的结果
        //1293
        //253
        //211
        //182
        //177

        connection.close();
    }
}
