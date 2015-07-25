package com.alibaba.middleware.race.rpc.api.serializer;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by Dawnwords on 2015/7/22.
 */
public enum SerializeType {
    java(new JavaSerializer());

    private Serializer serializer;

    SerializeType(Serializer serializer) {
        this.serializer = serializer;
    }

    public void serialize(Object obj, OutputStream out) throws Exception {
        serializer.serialize(obj, out);
    }

    public Object deserialize(InputStream in) throws Exception {
        return serializer.deserialize(in);
    }
}
