package com.atguigu.utils;

import lombok.SneakyThrows;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池工具类: 创建ThreadPoolExecutor类的单例对象
 * 单例模式（线程安全的懒汉式的）
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                     threadPoolExecutor = new ThreadPoolExecutor(4,
                            20,   //corePoolSize、maximumPoolSize的值在公司中是通过压测确定的
                            60,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>()); //当corePoolSize个线程都在使用时，当有新的任务时，不是立刻创建新的线程，而是先放入队列中，当队列满的时候才会开始给任务分配线程（已有的或新建的）
                }
            }
        }

        return threadPoolExecutor;
    }

    /**
     * 验证上面第28行的注释内容
     * @param args
     */
    public static void main(String[] args) {
        ThreadPoolExecutor threadPoolExecutor = getThreadPoolExecutor();

        for (int i = 0; i < 10; i++) {
            threadPoolExecutor.execute(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                    Thread.sleep(2000);
                }
            });
        }

    }
}
