package com.atguigu.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private String fileName;
    private Text outK = new Text();
    private TableBean outV = new TableBean();

    //通过Mapper.Context对象获取FileSplit对象，再由此对象获取当前处理的文件的文件名
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        FileSplit fileSpilt = (FileSplit) context.getInputSplit();
        fileName = fileSpilt.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //判断文件名 进行不同的处理
        if (fileName.contains("order")) {
            String[] orderInfo = line.split("\t");
            outK.set(orderInfo[1]);
            outV.setId(orderInfo[0]);
            outV.setPid(orderInfo[1]);
            outV.setAmount(Integer.parseInt(orderInfo[2]));
            outV.setPname("");
            outV.setFlag("order");
            context.write(outK, outV);
        } else {
            String[] pdInfo = line.split(" ");
            outK.set(pdInfo[0]);
            outV.setId("");
            outV.setPid(pdInfo[0]);
            outV.setAmount(0);
            outV.setPname(pdInfo[1]);
            outV.setFlag("pd");
            context.write(outK, outV);
        }

    }
}
