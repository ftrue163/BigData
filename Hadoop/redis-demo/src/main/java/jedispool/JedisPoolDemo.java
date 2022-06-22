package jedispool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

public class JedisPoolDemo {
    public static void main(String[] args) {
        //声明Linux服务器IP地址
        String host = "hadoop102";

        //声明Redis端口号
        int port = Protocol.DEFAULT_PORT;

        //创建连接池对象
        JedisPool jedisPool = new JedisPool(host, port);

        //获取Jedis对象连接Redis
        Jedis jedis = jedisPool.getResource();

        //执行具体操作
        String ping = jedis.ping();

        System.out.println(ping);

        //关闭连接
        jedisPool.close();

    }
}
