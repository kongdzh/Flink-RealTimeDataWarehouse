package com.yankee.gmall.realtime.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ThreadPoolUtil {
    // 声明线程池
    public static ThreadPoolExecutor poolExecutor;

    // 私有化构造器
    private ThreadPoolUtil() {
    }

    // 单例对象，懒汉式实现，双重检验
    public static ThreadPoolExecutor getInstance() {
        if (poolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (poolExecutor == null) {
                    log.info("开辟线程池！");
                    poolExecutor = new ThreadPoolExecutor(4, 20, 30L, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>(Integer.MAX_VALUE));
                }
            }
        }
        return poolExecutor;
    }
}
