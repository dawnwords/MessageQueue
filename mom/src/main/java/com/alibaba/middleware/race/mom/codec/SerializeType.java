package com.alibaba.middleware.race.mom.codec;

/**
 * Created by Dawnwords on 2015/7/22.
 */
public enum SerializeType {
    java(new ObjectSerializer()),
    kryo(new KryoSerializer());

    private final Serializer serializer;

    SerializeType(Serializer serializer) {
        this.serializer = serializer;
    }

    public Serializer serializer() {
        return serializer;
    }
}
