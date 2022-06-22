package com.atguigu.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int sum;
    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //累加求和
        sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //输出
        outV.set(sum);
        context.write(key, outV);
    }
}
