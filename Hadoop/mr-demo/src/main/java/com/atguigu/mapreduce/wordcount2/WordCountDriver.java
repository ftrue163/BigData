package com.atguigu.mapreduce.wordcount2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 通过IDEA将MR任务提交到集群中执行
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //通过配置文件获取job的实例
        Configuration conf = new Configuration();
        //设置HDFS NameNode的地址
        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");
        //指定MapReduce运行在Yarn上
        conf.set("mapreduce.framework.name","yarn");
        //指定mapreduce可以在远程集群运行
        conf.set("mapreduce.app-submission.cross-platform","true");
        //指定Yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname","hadoop103");
        Job job = Job.getInstance(conf);
        //关联本Driver程序的jar
        //job.setJarByClass(WordCountDriver.class);  //此时必须用下面的方法指定jar包的位置
        job.setJar("E:\\IdeaProjects\\Bigdata210625\\Hadoop\\mr-demo\\target\\mr-demo-1.0-SNAPSHOT.jar");
        //关联Mapper和Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop102:8020/input/phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop102:8020/output/flow"));
        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
