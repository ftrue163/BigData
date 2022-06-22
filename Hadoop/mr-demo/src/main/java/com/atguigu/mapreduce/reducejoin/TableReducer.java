package com.atguigu.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    private ArrayList<TableBean> orderList = new ArrayList<>();
    private TableBean pdBean = new TableBean();

    //此处的增强for循环，注意复制值的处理方式
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        //集合清零
        orderList.clear();
        // 1001  01  1
        // 1004  01 4
        //       01     小米
        //遍历集合 不同数据进行不同处理 而且你还不知道数据谁先过来
        for (TableBean value : values) {
            if ("order".equals(value.getFlag())) {
                TableBean tmpBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderList.add(tmpBean);
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        //替换
        for (TableBean tableBean : orderList) {
            tableBean.setPname(pdBean.getPname());
            context.write(tableBean, NullWritable.get());
        }
    }
}
