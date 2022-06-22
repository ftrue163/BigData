package com.atguigu.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream otherfos;
    private FSDataOutputStream atguigufos;

    public LogRecordWriter(TaskAttemptContext job) throws IOException {
        //注意不要new Configuration对象，而是通过job获取
        FileSystem fs = FileSystem.get(job.getConfiguration());
        atguigufos = fs.create(new Path("E:\\output\\log\\atguigu.txt"));
        otherfos = fs.create(new Path("E:\\output\\log\\other.txt"));
    }

    //每一个reduce输出就调用一次
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //转化字符串
        String log = key.toString();
        //判断是否包含atguigu
        if (log.contains("atguigu")) {
            atguigufos.writeBytes(log + "\n");
        } else {
            otherfos.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        otherfos.close();
        atguigufos.close();
    }
}
