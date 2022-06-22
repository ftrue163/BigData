package com.atguigu.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {
    //创建Logger对象
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);

    private String prefix;
    private String suffix;

    @Override
    public void configure(Context context) {
        //读取配置文件内容，有默认值
        prefix = context.getString("pre", "pre-");

        //读取配置文件内容，无默认值
        suffix = context.getString("suf");
    }

    @Override
    public Status process() throws EventDeliveryException {
        //声明返回值状态信息
        Status status;

        //获取当前Sink绑定的Channel
        Channel ch = getChannel();

        //获取事务
        Transaction txn = ch.getTransaction();

        //开启事务
        txn.begin();

        try {
            //2.1抓取数据
            Event event;
            while (true) {
                event = ch.take();
                if (event != null) {
                    break;
                }
            }

            //2.2处理数据
            LOG.info(prefix + new String(event.getBody()) + suffix);

            //2.3提交事务
            txn.commit();
            status = Status.READY;
        } catch (Exception e) {
            //遇到异常，事务回滚
            txn.rollback();
            status = Status.BACKOFF;
        } finally {
            //关闭事务
            txn.close();
        }
        return status;
    }
}
