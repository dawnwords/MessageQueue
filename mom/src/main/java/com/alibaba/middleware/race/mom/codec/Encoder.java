package com.alibaba.middleware.race.mom.codec;

import com.alibaba.middleware.race.mom.bean.SerializeWrapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by Dawnwords on 2015/8/8.
 */
public class Encoder extends MessageToByteEncoder {
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        int startIdx = out.writerIndex();
        out.writeInt(0);
        ((SerializeWrapper) msg).encode(out);
        int endIdx = out.writerIndex();
        out.setInt(startIdx, endIdx - startIdx - 4);
    }
}
