package com.alibaba.middleware.race.rpc.api;

import com.alibaba.middleware.race.rpc.api.codec.SerializeType;

/**
 * Created by Dawnwords on 2015/7/22.
 */
public class Parameter {
    public static final String SERVER_IP = "127.0.0.1";
    public static final int BACKLOG_SIZE = 1024;
    public static final int SERVER_PORT = 8888;
    public static final int SERVER_SELECTOR_THREADS = 3;
    public static final int SERVER_WORKER_THREADS = 8;

    public static final int SND_BUF_SIZE = 65535;
    public static final int RCV_BUF_SIZE = 65535;

    public static final SerializeType SERIALIZE_TYPE = SerializeType.kryo;
}
