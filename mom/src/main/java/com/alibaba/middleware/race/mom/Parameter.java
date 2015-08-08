package com.alibaba.middleware.race.mom;

import com.alibaba.middleware.race.mom.store.DummyStorage;
import com.alibaba.middleware.race.mom.store.Storage;

/**
 * Created by Dawnwords on 2015/7/22.
 */
public class Parameter {
    public static final String SERVER_IP = "127.0.0.1";
    public static final int BACKLOG_SIZE = 1024;
    public static final int SERVER_PORT = 9999;
    public static final int PROCESSORS = Runtime.getRuntime().availableProcessors();
    public static final int SERVER_BOSS_THREADS = PROCESSORS;
    public static final int SERVER_WORKER_THREADS = PROCESSORS * 2;
    public static final int SERVER_EXECUTOR_THREADS = PROCESSORS * 2;

    public static final int CLIENT_THREADS = PROCESSORS;

    public static final int SND_BUF_SIZE = 65535;
    public static final int RCV_BUF_SIZE = 65535;

    public static final Storage STORAGE = new DummyStorage();

    public static final int RESEND_NUM = 100;

    public static final int PRODUCER_TIME_OUT_SECOND = 10;
}