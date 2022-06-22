package com.atguigu.mapreduce.wordcount1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * 1.继承mapper类
 * 2.思考泛型应该是什么
 *           4个
 *           两对
 *               1对输入
 *                        keyin      读取数据的位置 偏移量  LongWritable
 *                        valuein    那一行数据              Text
 *               1对输出
 *                       keyout      单词本身   Text
 *                       valueout    1          IntWritable
 * 3.通过需求写逻辑
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text outK = new Text();
    IntWritable outV = new IntWritable(1);

    /**
     * 参数解读
     * @param key  偏移量
     * @param value  读进来的一行数据
     * @param context 全局的上下文对象  将数据送给reducer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.将传进来的数据  转化成STRING  atguigu atguigu
        String line = value.toString();
        //2.将数据按照空格切分
        String[] words = line.split(" ");
        //3.将数据循环并写出
        for (String word : words) {
            outK.set(word);
            context.write(outK, outV);
        }
    }
}
