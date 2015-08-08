package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.ConsumeStatus;
import com.alibaba.middleware.race.mom.util.ByteUtil;
import io.netty.buffer.ByteBuf;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class ConsumeResultWrapper implements SerializeWrapper<ConsumeResult> {
    private byte status;
    private byte[] info;

    @Override
    public ConsumeResult deserialize() {
        ConsumeResult result = new ConsumeResult();
        result.setStatus(ConsumeStatus.values()[status]);
        result.setInfo(ByteUtil.toString(info));
        return result;
    }

    @Override
    public ConsumeResultWrapper serialize(ConsumeResult consumeResult) {
        this.status = (byte) consumeResult.getStatus().ordinal();
        this.info = ByteUtil.toBytes(consumeResult.getInfo());
        return this;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeByte(CONSUME_RESULT);
        out.writeByte(status);
        Encoder.encode(out, info);
    }

    @Override
    public ConsumeResultWrapper decode(ByteBuf in) {
        status = in.readByte();
        info = Decoder.decode(in);
        return this;
    }
}
