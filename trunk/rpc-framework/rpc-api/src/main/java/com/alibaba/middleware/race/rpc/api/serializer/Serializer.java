package com.alibaba.middleware.race.rpc.api.serializer;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by Dawnwords on 2015/7/22.
 */
public interface Serializer {
    void serialize(Object obj, OutputStream out) throws Exception;

    Object deserialize(InputStream in) throws Exception;
}
