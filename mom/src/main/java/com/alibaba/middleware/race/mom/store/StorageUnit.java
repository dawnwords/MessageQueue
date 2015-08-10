package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.bean.MessageId;

import java.nio.ByteBuffer;

/**
 * Storage Unit
 * <p/>
 * 0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
 * +----------------------------------------------+
 * |    ip    |    port   |        born time      |
 * +----------------------------------------------+
 * |   length |          offset       |   state   |
 * +----------------------------------------------+
 * |                   content                    |
 * +----------------------------------------------+
 */

public class StorageUnit {
    public static final int HEADER_LENGTH = 32;

    private ByteBuffer header;
    private ByteBuffer body;

    public ByteBuffer header() {
        return header;
    }

    public StorageUnit header(ByteBuffer header) {
        this.header = header;
        return this;
    }

    public ByteBuffer body() {
        return body;
    }

    public StorageUnit body(ByteBuffer body) {
        this.body = body;
        return this;
    }

    public MessageId msgId() {
        byte[] msgIdBytes = new byte[MessageId.LENGTH];
        header.get(msgIdBytes);
        header.limit(HEADER_LENGTH);
        header.position(0);
        return new MessageId(msgIdBytes);
    }
}
