package com.atguigu.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        //转化字符串
        String line = value.toString();
        Boolean parseLog = parseLog(line);
        if (parseLog) {
            context.write(value, NullWritable.get());
        }
    }

    private Boolean parseLog(String line) {
        String[] words = line.split(" ");
        if (words.length > 11) {
            return true;
        }
        return false;
    }
}
