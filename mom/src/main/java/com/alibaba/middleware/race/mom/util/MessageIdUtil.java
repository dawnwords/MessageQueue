package com.alibaba.middleware.race.mom.util;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Created by Dawnwords on 2015/8/8.
 */
public class MessageIdUtil {

    public static byte[] createMesageId(InetSocketAddress address, long bornTime) {
        ByteBuffer bytes = ByteBuffer.allocate(16);
        bytes.put(address.getAddress().getAddress());
        bytes.putInt(address.getPort());
        bytes.putLong(bornTime);
        return bytes.array();
    }

    public static String toString(byte[] msgId) {
        ByteBuffer buffer = ByteBuffer.wrap(msgId);
        return buffer.getInt() + "-" + buffer.getInt() + "-" + buffer.getLong();
    }
}
