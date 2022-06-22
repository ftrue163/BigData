package com.atguigu.mapreduce.mapjoin;

import com.atguigu.mapreduce.reducejoin.TableBean;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private HashMap<String, String> pdMap;
    private Text outK = new Text();

    //任务开始前将pd数据缓存进pdMap
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //创建一个内存中的数据结构来缓存小表的数据
        pdMap = new HashMap<String, String>();
        //通过上下文对象获取缓存文件
        URI[] cacheFiles = context.getCacheFiles();
        URI cacheFile = cacheFiles[0];

        //开流读取数据
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fdis = fs.open(new Path(cacheFile));

        //字符缓冲流
        BufferedReader reader = new BufferedReader(new InputStreamReader(fdis, "utf-8"));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] pdInfo = line.split("\t");
            pdMap.put(pdInfo[0], pdInfo[1]);
        }

        //关闭资源
        //fs不能关闭，后面代码运行还会用到此对象
        reader.close();
        fdis.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //读的是大表
        String line = value.toString();
        String[] orderInfo = line.split("\t");
        String pname = pdMap.get(orderInfo[1]);
        outK.set(orderInfo[0] + "\t" + pname + "\t" + orderInfo[2]);
        context.write(outK, NullWritable.get());
    }
}
