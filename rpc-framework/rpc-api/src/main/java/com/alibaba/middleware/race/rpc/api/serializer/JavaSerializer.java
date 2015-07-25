package com.alibaba.middleware.race.rpc.api.serializer;

import java.io.*;

/**
 * Created by Dawnwords on 2015/7/22.
 */
public class JavaSerializer implements Serializer {

    @Override
    public void serialize(Object obj, OutputStream out) throws Exception {
        if (obj == null) {
            throw new NullPointerException("object is null1");
        }
        if(out == null){
            throw new NullPointerException("output stream is null");
        }
        new ObjectOutputStream(out).writeObject(obj);
    }

    @Override
    public Object deserialize(InputStream in) throws Exception {
        if (in == null) {
            throw new NullPointerException("input stream is null");
        }
        return new ObjectInputStream(in).readObject();
    }
}
