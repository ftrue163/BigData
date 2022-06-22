package jedis;

import redis.clients.jedis.Jedis;

public class JedisTest {
    public static void main(String[] args) {
        //指定Redis服务器的IP地址和端口号
        Jedis jedis = new Jedis("hadoop102", 6379);

        //执行ping命令
        String ping = jedis.ping();

        System.out.println(ping);
        System.out.println(jedis.keys("*"));

        //关闭连接
        jedis.close();
    }
}
