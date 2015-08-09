package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.ConsumeStatus;
import io.netty.buffer.ByteBuf;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class ConsumeResultWrapper implements SerializeWrapper<ConsumeResult> {
    private byte status;
    private byte[] msgId;
    private byte[] info;

    @Override
    public ConsumeResult deserialize() {
        ConsumeResult result = new ConsumeResult().msgId(new MessageId(msgId));
        result.setStatus(ConsumeStatus.values()[status]);
        result.setInfo(Bytes.toString(info));
        return result;
    }

    @Override
    public ConsumeResultWrapper serialize(ConsumeResult consumeResult) {
        this.status = (byte) consumeResult.getStatus().ordinal();
        this.info = Bytes.toBytes(consumeResult.getInfo());
        this.msgId = consumeResult.msgId().id();
        return this;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeByte(CONSUME_RESULT);
        out.writeByte(status);
        out.writeBytes(msgId);
        Encoder.encode(out, info);
    }

    @Override
    public ConsumeResultWrapper decode(ByteBuf in) {
        status = in.readByte();
        msgId = new byte[16];
        in.readBytes(msgId);
        info = Decoder.decode(in);
        return this;
    }
}
