package com.alibaba.middleware.race.mom.codec;

import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;

/**
 * Created by Dawnwords on 2015/7/31.
 */
public interface Serializer {
    Object decode(byte[] bytes);

    byte[] encode(Object o);

    ChannelInboundHandler decoder();

    ChannelOutboundHandler encoder();
}
