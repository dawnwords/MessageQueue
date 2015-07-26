package com.alibaba.middleware.race.rpc.api.codec;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

/**
 * Created by Dawnwords on 2015/7/26.
 */
public class ObjectSerializer implements SerializerFactory {
    @Override
    public ByteToMessageDecoder deserializer() {
        return new ObjectDecoder(ClassResolvers.cacheDisabled(SerializeType.class.getClassLoader()));
    }

    @Override
    public MessageToByteEncoder serializer() {
        return new ObjectEncoder();
    }
}
