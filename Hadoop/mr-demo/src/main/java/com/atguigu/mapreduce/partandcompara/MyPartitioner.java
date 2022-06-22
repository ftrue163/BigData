package com.atguigu.mapreduce.partandcompara;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        String phone = text.toString();
        //前三位
        String prePhone = phone.substring(0, 3);
        int part = 0;
        //判断
        switch (prePhone) {
            case "136":
                part = 0;
                break;
            case "137":
                part = 1;
                break;
            case "138":
                part = 2;
                break;
            case "139":
                part = 3;
                break;
            default:
                part = 4;
        }
        return part;
    }
}
