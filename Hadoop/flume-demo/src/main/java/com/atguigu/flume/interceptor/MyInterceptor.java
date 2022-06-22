package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {

    //声明一个集合用于存放拦截器处理后的事件
    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {
        //初始化集合用于存放拦截器处理后的事件
        addHeaderEvents = new ArrayList<>();
    }

    //单个事件拦截
    @Override
    public Event intercept(Event event) {
        //1.获取事件中的头信息
        Map<String, String> headers = event.getHeaders();

        //2.获取事件中的body信息
        String body = new String(event.getBody());

        //3.根据body中是否有"atguigu"来决定添加怎样的头信息
        if (body.contains("atguigu")) {
            //4.添加头信息
            headers.put("type", "atguigu");
        } else {
            //4.添加头信息
            headers.put("type", "other");
        }

        return event;

    }

    //批量事件拦截
    @Override
    public List<Event> intercept(List<Event> events) {
        //putlist takelist 和此方法没有关系
        //设置该 batchsize() 参数就会调用该方法

        //1.清空集合
        addHeaderEvents.clear();

        //2.遍历events
        for (Event event : events) {
            addHeaderEvents.add(intercept(event));
        }

        //3.返回数据
        return addHeaderEvents;
    }

    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
