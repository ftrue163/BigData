package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 驱动类
 *   main方法所在类
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.通过配置文件获取job的实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.绑定当前driver或者jar包
        job.setJarByClass(WordCountDriver.class);
        //3.绑定当前mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4.设定mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.设定最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设定一下redcue的个数
        job.setNumReduceTasks(0);
        //combiner设置
        job.setCombinerClass(WordCountReducer.class);

        //6.指定当前job的读入数据
        FileInputFormat.setInputPaths(job, new Path("D:\\input\\hello.txt"));
        //7.指定当前job的输出数据
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoop\\wc7"));
        //8.提交运行
        boolean b = job.waitForCompletion(true);
        //System.exit(b?0:1);
    }
}
