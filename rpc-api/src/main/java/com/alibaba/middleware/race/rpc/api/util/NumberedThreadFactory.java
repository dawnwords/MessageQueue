package com.alibaba.middleware.race.rpc.api.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Dawnwords on 2015/7/27.
 */
public class NumberedThreadFactory implements ThreadFactory {
    private String prefix;
    private AtomicInteger threadIdx;

    public NumberedThreadFactory(String prefix) {
        this.prefix = prefix;
        this.threadIdx = new AtomicInteger(0);
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, prefix + threadIdx.getAndIncrement());
    }
}
