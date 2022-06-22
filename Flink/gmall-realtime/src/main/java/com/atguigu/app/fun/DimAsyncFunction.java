package com.atguigu.app.fun;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {
    private ThreadPoolExecutor threadPoolExecutor;
    private Connection connection;
    private String table;

    public DimAsyncFunction(String table) {
        this.table = table;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                //抽象方法  通过实现类实现
                //获取key
                String key = getKey(input);

                try {
                    //读取维度数据
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, table, key);

                    //抽象方法  通过实现类实现
                    //将维度信息补充至数据
                    if (dimInfo != null) {
                        join(input, dimInfo);
                    }

                    //将补充完成的数据写出
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }

    protected abstract void join(T input, JSONObject dimInfo) throws ParseException;

    public abstract String getKey(T input);

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
