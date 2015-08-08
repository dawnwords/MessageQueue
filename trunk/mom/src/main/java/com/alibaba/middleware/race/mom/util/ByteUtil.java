package com.alibaba.middleware.race.mom.util;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Created by Dawnwords on 2015/8/8.
 */
public class ByteUtil {

    public static byte[] createMessageId(InetSocketAddress address, long bornTime) {
        ByteBuffer bytes = ByteBuffer.allocate(16);
        bytes.put(address.getAddress().getAddress());
        bytes.putInt(address.getPort());
        bytes.putLong(bornTime);
        return bytes.array();
    }

    public static String messageId2String(byte[] msgId) {
        ByteBuffer buffer = ByteBuffer.wrap(msgId);
        return buffer.getInt() + "-" + buffer.getInt() + "-" + buffer.getLong();
    }

    public static byte[] toBytes(String s) {
        return s == null ? null : s.getBytes();
    }

    public static String toString(byte[] bytes) {
        return bytes == null ? null : new String(bytes);
    }
}
