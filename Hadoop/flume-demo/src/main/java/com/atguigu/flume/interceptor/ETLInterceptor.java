package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class ETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        //判断数据的结构是否完整，即是否符合JSON数据结构标准
        //将不符合的数据过滤掉
        if (JSONUtils.isJSONValidate(log)) {
            return event;
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        //清楚集合中的元素的两种常见方式

        //方式1
        /*Iterator<Event> iterator = events.iterator();

        while (iterator.hasNext()) {
            if (intercept(iterator.next()) == null) {
                iterator.remove();
            }
        }*/

        //方式2
        events.removeIf(event -> intercept(event) == null);

        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
