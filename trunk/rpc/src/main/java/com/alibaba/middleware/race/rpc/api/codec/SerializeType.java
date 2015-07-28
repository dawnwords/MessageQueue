package com.alibaba.middleware.race.rpc.api.codec;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by Dawnwords on 2015/7/22.
 */
public enum SerializeType {
    java(new ObjectSerializer()),
    kryo(new KryoSerializer());

    private final SerializerFactory factory;

    SerializeType(SerializerFactory factory) {
        this.factory = factory;
    }

    public ByteToMessageDecoder deserializer() {
        return factory.deserializer();
    }

    public MessageToByteEncoder serializer() {
        return factory.serializer();
    }
}
