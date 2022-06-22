package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogReducer extends Reducer<NullWritable, Text, Text, NullWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Reducer<NullWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}
