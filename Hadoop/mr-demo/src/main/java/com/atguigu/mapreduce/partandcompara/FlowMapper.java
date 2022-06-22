package com.atguigu.mapreduce.partandcompara;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    FlowBean outK = new FlowBean();
    Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //分隔
        String[] phones = line.split("\t");
        //获取手机号
        String phone = phones[0];
        //封装
        outV.set(phone);
        outK.setUpFlow(Integer.parseInt(phones[1]));
        outK.setDownFlow(Integer.parseInt(phones[2]));
        outK.setSumFlow();
        //写出
        context.write(outK, outV);
    }
}
