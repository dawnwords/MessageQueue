package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.bean.MessageId;

import java.nio.ByteBuffer;

/**
 * Storage Unit
 * <p/>
 * 0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
 * +----------------------------------------------+
 * |    ip    |    port   |          id           |
 * +----------------------------------------------+
 * |  length  |   state   |  topicLen |  ~topic~  |
 * +----------------------------------------------+
 * |  propNum | propkeyLen| ~propkey~ | propValLen|
 * +----------------------------------------------+
 * | ~propVal~|       bornTime        |  bodyLen  |
 * +----------------------------------------------+
 * |                 ~ body~                      |
 * +----------------------------------------------+
 */

public class StorageUnit {
    public static final int HEADER_LENGTH = 24;
    public static final int STATE_OFFSET = 20;

    private ByteBuffer msg;

    public ByteBuffer msg() {
        return msg;
    }

    public StorageUnit msg(ByteBuffer msg) {
        this.msg = msg;
        return this;
    }

    public MessageId msgId() {
        byte[] msgIdBytes = new byte[MessageId.LENGTH];
        msg.position(0);
        msg.get(msgIdBytes);
        msg.position(0);
        return new MessageId(msgIdBytes);
    }

    @Override
    public String toString() {
        return "StorageUnit{" + msgId() + '}';
    }
}
