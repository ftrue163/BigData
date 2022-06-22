package com.atguigu.mapreduce.wordcount2;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * 1.继承reducer类
 * 2.思考泛型是什么
 *            4个  两对
 *            1对输入
 *                   肯定是mapper的输出
 *
 *            1对输出
 *                   单词     Text
 *                   总次数   IntWritable
 *
 * 3.想一下业务逻辑该怎么写
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outV = new IntWritable();

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
