package com.alibaba.middleware.race.rpc.api.codec;

import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by Dawnwords on 2015/7/26.
 */
interface SerializerFactory {
    ByteToMessageDecoder deserializer();

    MessageToByteEncoder serializer();
}
