package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.ConsumeStatus;
import com.alibaba.middleware.race.mom.codec.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class ConsumeResultWrapper implements SerializeWrapper<ConsumeResult> {
    private byte status;
    private byte[] info;

    @Override
    public ConsumeResult deserialize(Serializer serializer) {
        ConsumeResult result = new ConsumeResult();
        result.setStatus(ConsumeStatus.values()[status]);
        result.setInfo((String) serializer.decode(info));
        return result;
    }

    @Override
    public SerializeWrapper<ConsumeResult> serialize(ConsumeResult consumeResult, Serializer serializer) {
        this.status = (byte) consumeResult.getStatus().ordinal();
        this.info = serializer.encode(consumeResult.getInfo());
        return this;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeByte(CONSUME_RESULT);
        out.writeByte(status);
        Encoder.encode(out, info);
    }

    @Override
    public SerializeWrapper<ConsumeResult> decode(ByteBuf in) {
        status = in.readByte();
        info = Decoder.decode(in);
        return this;
    }
}
