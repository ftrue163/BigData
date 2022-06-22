package com.atguigu.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

public class MySource extends AbstractSource implements Configurable, PollableSource {
    //声明数据的前后缀
    private String prefix;
    private String suffix;
    private Long delay;

    @Override
    public void configure(Context context) {
        prefix = context.getString("pre", "pre-");
        suffix = context.getString("suf");
        delay = context.getLong("delay", 2000L);
    }

    @Override
    public Status process() throws EventDeliveryException {
        try {
            //2.循环创建事件信息，传给channel
            for (int i = 0; i < 5; i++) {
                //1.声明事件
                Event event = new SimpleEvent();
                HashMap<String, String> header = new HashMap<>();
                //给事件设置头信息
                event.setHeaders(header);
                //给事件设置内容
                event.setBody((prefix + "atguigu: " + i + suffix).getBytes());
                //将事件写入channel
                getChannelProcessor().processEvent(event);
                //getChannelProcessor().processEventBatch();
                Thread.sleep(delay);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }

        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
