package com.alibaba.middleware.race.rpc.api.codec;

import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;

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

    public ChannelInboundHandler deserializer() {
        return factory.deserializer();
    }

    public ChannelOutboundHandler serializer() {
        return factory.serializer();
    }
}
