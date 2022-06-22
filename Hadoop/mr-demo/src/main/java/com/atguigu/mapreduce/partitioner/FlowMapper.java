package com.atguigu.mapreduce.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class FlowMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    private Text outK;
    private FlowBean outV;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       outK=new Text();
       outV=new FlowBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //转化字符串
        String line = value.toString();
        String[] phones = line.split("\t");
        String phone = phones[1];
        String upFlow = phones[phones.length - 3];
        String downFlow = phones[phones.length - 2];
        //进行封装
        outK.set(phone);
        outV.setUpFlow(Integer.parseInt(upFlow));
        outV.setDownFlow(Integer.parseInt(downFlow));
        outV.setSumFlow();
        //写出
        context.write(outK,outV);
    }
}
