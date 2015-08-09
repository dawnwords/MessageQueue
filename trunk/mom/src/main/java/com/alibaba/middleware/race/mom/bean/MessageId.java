package com.alibaba.middleware.race.mom.bean;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created by Dawnwords on 2015/8/9.
 */
public class MessageId implements Serializable {
    private byte[] id;

    public MessageId(byte[] id) {
        this.id = id;
    }

    public MessageId(InetSocketAddress address, long bornTime) {
        ByteBuffer bytes = ByteBuffer.allocate(16);
        bytes.put(address.getAddress().getAddress());
        bytes.putInt(address.getPort());
        bytes.putLong(bornTime);
        this.id = bytes.array();
    }

    @Override
    public String toString() {
        ByteBuffer buffer = ByteBuffer.wrap(id);
        return buffer.getInt() + "-" + buffer.getInt() + "-" + buffer.getLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageId messageId = (MessageId) o;

        return Arrays.equals(id, messageId.id);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(id);
    }

    public byte[] id() {
        return id;
    }
}
