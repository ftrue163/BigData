package com.atguigu.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable outV = new IntWritable();

    /**
     * @param key       每一组的单词
     * @param values    当前单词对应的所有value
     * @param context   上下文对象  将数据写出到文件中
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //定义一个求和变量
        int sum = 0;
        //atguigu [1,1]
        for (IntWritable value : values) {
            sum += value.get();
        }
        outV.set(sum);
        //写出
        context.write(key, outV);
    }
}
