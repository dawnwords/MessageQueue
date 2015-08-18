package com.alibaba.middleware.race.mom.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by scidb on 15-8-18.
 */
class ByteBufferPool {
    private ArrayList<Queue<ByteBuffer>> BUFFER_POOL;

    private static final ByteBufferPool instance = new ByteBufferPool();

    public static ByteBufferPool instance() {
        return instance;
    }

    private ByteBufferPool() {
        BUFFER_POOL = new ArrayList<Queue<ByteBuffer>>();
        for (int i = 0; i < 8; i++) {
            BUFFER_POOL.add(new ConcurrentLinkedQueue<ByteBuffer>());
        }
    }

    public ByteBuffer get(int size) {
        int group = size - 1 >> 12;
        Queue<ByteBuffer> byteBuffers = BUFFER_POOL.get(group);
        ByteBuffer buffer = byteBuffers.poll();
        return buffer != null ? buffer : ByteBuffer.allocate(group + 1 << 12);
    }

    public void release(ByteBuffer byteBuffer) {
        int group = (byteBuffer.limit() >> 12) - 1;
        byteBuffer.clear();
        BUFFER_POOL.get(group).add(byteBuffer);
    }
}
