package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.codec.Serializer;
import io.netty.buffer.ByteBuf;

import java.io.Serializable;

/**
 * Created by Dawnwords on 2015/7/31.
 */
public interface SerializeWrapper<T> extends Serializable {
    int REGISTER = 0;
    int MESSAGE = 1;
    int CONSUME_RESULT = 2;
    int SEND_RESULT = 4;

    T deserialize(Serializer serializer);

    SerializeWrapper<T> serialize(T obj, Serializer serializer);

    void encode(ByteBuf out);

    SerializeWrapper<T> decode(ByteBuf in);

    class Decoder {
        static byte[] decode(ByteBuf in) {
            int len = in.readInt();
            if (len == 0) return null;
            byte[] result = new byte[len];
            in.readBytes(result);
            return result;
        }

        static byte[][] decodeArray(ByteBuf in) {
            int len = in.readInt();
            if (len == 0) return null;
            byte[][] result = new byte[len][];
            for (int i = 0; i < result.length; i++) {
                result[i] = decode(in);
            }
            return result;
        }
    }

    class Encoder {
        static void encode(ByteBuf out, byte[] bytes) {
            if (bytes == null) {
                out.writeInt(0);
            } else {
                out.writeInt(bytes.length);
                out.writeBytes(bytes);
            }
        }

        static void encode(ByteBuf out, byte[][] bytes) {
            if (bytes == null) {
                out.writeInt(0);
            } else {
                out.writeInt(bytes.length);
                for (byte[] b : bytes) {
                    encode(out, b);
                }
            }
        }
    }
}
