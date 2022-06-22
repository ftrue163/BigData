package com.atguigu.mapreduce.flowbean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 1.继承reducer类
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outV = new FlowBean();
    }

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //totalUpFlow totalDownFlow
        int totalUpFlow = 0;
        int totalDownFlow = 0;
        //迭代数据
        for (FlowBean value : values) {
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
        }
        outV.setUpFlow(totalUpFlow);
        outV.setDownFlow(totalDownFlow);
        outV.setSumFlow();
        //写出
        context.write(key, outV);
    }
}
