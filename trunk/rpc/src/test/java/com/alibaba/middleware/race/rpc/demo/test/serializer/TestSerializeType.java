package com.alibaba.middleware.race.rpc.demo.test.serializer;

import com.alibaba.middleware.race.rpc.api.codec.SerializeType;

/**
 * Created by Dawnwords on 2015/7/29.
 */
public class TestSerializeType {
    public static SerializeType serializeType() {
        return SerializeType.kryo;
    }
}
