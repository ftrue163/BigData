package com.atguig.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class Hdfs_Client {

    private FileSystem fs;
    private Configuration conf;
    private URI uri;


    @Before   //在test方法运行之前 执行一次
    public void init() throws IOException, InterruptedException {
        uri = URI.create("hdfs://hadoop102:8020");
        conf = new Configuration();
        fs = FileSystem.get(uri, conf, "atguigu");
        conf.set("dfs.replication", "2");
    }

    @After  //在test方法之后执行一次
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void mkDir() throws IOException {
        fs.mkdirs(new Path("/input/java"));
    }

    @Test
    public void put() throws IOException {
        /**
         * 参数解读
         * 1.boolean delSrc 是否删除源文件 windows文件
         * 2.boolean overwrite 是否覆盖目标文件 hdfs文件
         * 3.Path src 源文件路径
         * 4.Path dst 目标文件路径
         */
        fs.copyFromLocalFile(false, true, new Path("E:\\input\\word1.txt"), new Path("/java/word.txt"));
        //参数优先级排序：（1）客户端代码中设置的值 >（2）ClassPath下的用户自定义配置文件 >（3）然后是服务器的自定义配置（xxx-site.xml） >（4）服务器的默认配置（xxx-default.xml）
    }

    @Test
    public void get() throws IOException {
        /**
         * boolean delSrc  是否删除源文件
         * Path src 下载文件路径
         * Path dst 目的文件路径
         * boolean useRawLocalFileSystem  true 不校验 false 校验
         *                                hdfs 默认会校验
         *                                但是 当你的本地文件系统校验开启后 hdfs不会校验
         */
        fs.copyToLocalFile(false, new Path("/java/word.txt"), new Path("E:\\input\\word2.txt"), false);
    }

    @Test
    public void mv() throws IOException {
        //文件的更名
        //fs.rename(new Path("/input/caocao.txt"),new Path("/input/mengde.txt"));
        //文件的移动
        //fs.rename(new Path("/input/mengde.txt"),new Path("/mengde.txt"));
        //文件的更名且移动
        //fs.rename(new Path("/mengde.txt"),new Path("/java/caocao.txt"));
        //目录的更名
        //fs.rename(new Path("/java"),new Path("/linux"));
        //目录的移动
        //fs.rename(new Path("/linux"),new Path("/input"));
        //目录的更名且移动
        fs.rename(new Path("/input/java"), new Path("/idea"));
    }

    @Test
    public void rm() throws IOException {
        fs.delete(new Path("/input"), true);
    }

    @Test
    public void ls() throws IOException {
        RemoteIterator<LocatedFileStatus> RemoteIterator = fs.listFiles(new Path("/"), true);
        while (RemoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = RemoteIterator.next();
            System.out.println("=============" + fileStatus.getPath() + "=============");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            //System.out.println(fileStatus.getModificationTime());
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println(simpleDateFormat.format(fileStatus.getModificationTime()));
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            //打印当前数据的块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    public void fileOrDir() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        //循环迭代
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件" + fileStatus.getPath());
            } else {
                System.out.println("目录" + fileStatus.getPath());
            }
        }
    }

    public void fileOrDir(Path path) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(path);
        //循环迭代
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("文件" + fileStatus.getPath());
            } else {
                System.out.println("目录" + fileStatus.getPath());
                fileOrDir(fileStatus.getPath());
            }
        }
    }

    @Test
    public void testFile() throws IOException {
        fileOrDir(new Path("/"));
    }

    @Test
    public void putByIO() throws IOException {
        //1.开本地文件输入流
        FileInputStream fis = new FileInputStream("D:\\input\\liangliang.txt");
        //2.开hdfs文件输出流
        FSDataOutputStream fdos = fs.create(new Path("/aliang.txt"));
        //3.流的对拷
        IOUtils.copyBytes(fis, fdos, conf);
        //4.流关闭
        IOUtils.closeStreams(fdos, fis);
    }

    @Test
    public void getByIO() throws IOException {
        //1.开启hdfs输入流
        FSDataInputStream fdis = fs.open(new Path("/idea/hadoop-3.1.3.tar.gz"));
        //2.开启本地文件输出流
        FileOutputStream fos = new FileOutputStream("D:\\input\\hadoop-3.1.3.tar.gz");
        //3.流的对拷
        IOUtils.copyBytes(fdis, fos, conf);
        //4.流的关闭
        IOUtils.closeStreams(fos, fdis);
    }

}
