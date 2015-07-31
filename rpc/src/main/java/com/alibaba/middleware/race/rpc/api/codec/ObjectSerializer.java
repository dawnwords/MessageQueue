package com.alibaba.middleware.race.rpc.api.codec;

import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.io.*;

/**
 * Created by Dawnwords on 2015/7/26.
 */
public class ObjectSerializer implements Serializer {

    @Override
    public Object decode(byte[] bytes) {
        if (bytes == null) return null;
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            close(ois);
        }
    }

    @Override
    public byte[] encode(Object o) {
        if (o == null) return null;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(o);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            close(bos);
            close(oos);
        }
    }

    private void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public ChannelOutboundHandler encoder() {
        return new ObjectEncoder();
    }

    @Override
    public ChannelInboundHandler decoder() {
        return new ObjectDecoder(ClassResolvers.cacheDisabled(ObjectSerializer.class.getClassLoader()));
    }
}
