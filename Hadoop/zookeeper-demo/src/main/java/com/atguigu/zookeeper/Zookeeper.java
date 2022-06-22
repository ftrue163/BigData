package com.atguigu.zookeeper;

import com.sun.xml.internal.bind.v2.TODO;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * 1.创建客户端对象 2.使用客户端对象进行操作 3.关闭客户端对象
 */
public class Zookeeper {

    private ZooKeeper zkClient;

    @Before  //获取zookeeper客户端
    public void init() throws IOException {
        String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 10000;

        //参数解读 1集群连接字符串  2连接超时时间 单位:毫秒  3当前客户端默认的监控器
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //方法在zookeeper客户端连接成功时会执行一次
                //方法在zookeeper客户端断开连接时会执行一次
            }
        });
    }

    @After  //关闭客户端对象
    public void close() throws InterruptedException {
        zkClient.close();
    }

    @Test  //获取子节点列表，不监听
    public void ls() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/sanguo", false);
        System.out.println(children);
    }

    @Test  //获取子节点列表，并监听
    public void lsAndWatch() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/sanguo", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });
        System.out.println(children);

        //因为设置了监听，所以当前线程不能结束
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test  //创建子节点
    public void create() throws InterruptedException, KeeperException {
        //参数解读 1节点路径  2节点存储的数据  3节点的权限(使用Ids选个OPEN即可)  4节点类型 短暂 持久 短暂带序号 持久带序号
        String path = zkClient.create("/atguigu", "shangguigu".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        //创建临时节点
        //String path = zkClient.create("/atguigu2", "shanguigu".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        System.out.println(path);

        //创建临时节点的话,需要线程阻塞
        //Thread.sleep(10000);
    }

    @Test  //判断Znode是否存在
    public void exist() throws Exception {

        Stat stat = zkClient.exists("/atguigu", false);

        System.out.println(stat == null ? "not exist" : "exist");
    }


    @Test  //获取子节点存储的数据,不监听
    public void get() throws KeeperException, InterruptedException {
        //判断节点是否存在
        Stat stat = zkClient.exists("/atguigu", false);
        if (stat == null) {
            System.out.println("节点不存在...");
            return;
        }

        byte[] data = zkClient.getData("/atguigu", false, stat);
        System.out.println(new String(data));
    }


    @Test  //获取子节点存储的数据,并监听
    public void getAndWatch() throws KeeperException, InterruptedException {
        //判断节点是否存在
        Stat stat = zkClient.exists("/atguigu", false);
        if (stat == null) {
            System.out.println("节点不存在...");
            return;
        }

        byte[] data = zkClient.getData("/atguigu", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        }, stat);
        System.out.println(new String(data));
        //线程阻塞
        Thread.sleep(Long.MAX_VALUE);
    }


    @Test  //设置节点的值
    public void set() throws KeeperException, InterruptedException {
        //判断节点是否存在
        Stat stat = zkClient.exists("/atguigu", false);
        if (stat == null) {
            System.out.println("节点不存在...");
            return;
        }

        //参数解读 1节点路径 2节点的值 3版本号
        zkClient.setData("/atguigu", "sgg".getBytes(), stat.getVersion());
    }


    @Test  //删除空节点
    public void delete() throws KeeperException, InterruptedException {
        //判断节点是否存在
        Stat stat = zkClient.exists("/aaa", false);
        if (stat == null) {
            System.out.println("节点不存在...");
            return;
        }

        zkClient.delete("/aaa", stat.getVersion());
    }

    //删除非空节点,递归实现
    //封装一个方法,方便递归调用
    public void deleteAll(String path, ZooKeeper zk) throws KeeperException, InterruptedException {
        //判断节点是否存在
        Stat stat = zkClient.exists(path, false);
        if (stat == null) {
            System.out.println("节点不存在...");
            return;
        }
        //先获取当前传入节点下的所有子节点
        List<String> children = zk.getChildren(path, false);
        if (children.isEmpty()) {
            //说明传入的节点没有子节点,可以直接删除
            zk.delete(path, stat.getVersion());
        } else {
            //如果传入的节点有子节点,循环所有子节点
            for (String child : children) {
                //删除子节点,但是不知道子节点下面还有没有子节点,所以递归调用
                deleteAll(path + "/" + child, zk);
            }
            //删除完所有子节点以后,记得删除传入的节点
            zk.delete(path, stat.getVersion());
        }
    }

    //测试deleteAll
    @Test
    public void testDeleteAll() throws KeeperException, InterruptedException {
        deleteAll("/atguigu", zkClient);
    }


    //获取子节点的列表，并循环监听
    public void lsAndWatch(String path) throws InterruptedException, KeeperException {
        //注意path变量的作用域范围
        List<String> children = zkClient.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
                System.out.println("0625 is best of atguigu");
                try {
                    lsAndWatch(path);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        });
        System.out.println(children);
    }

    @Test
    public void testWatch() throws KeeperException, InterruptedException {
        lsAndWatch("/atguigu");
        Thread.sleep(Long.MAX_VALUE);
    }

}
