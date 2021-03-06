package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.SendResult;
import com.alibaba.middleware.race.mom.SendStatus;
import io.netty.buffer.ByteBuf;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class SendResultWrapper implements SerializeWrapper<SendResult> {
    private byte status;
    private byte[] msgId;
    private byte[] info;

    @Override
    public SendResult deserialize() {
        SendResult result = new SendResult();
        result.setStatus(SendStatus.values()[status]);
        result.setMsgId(msgId);
        result.setInfo(Bytes.toString(info));
        return result;
    }

    @Override
    public SendResultWrapper serialize(SendResult sendResult) {
        this.status = (byte) sendResult.getStatus().ordinal();
        this.msgId = sendResult.getMessageId().id();
        this.info = Bytes.toBytes(sendResult.getInfo());
        return this;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeByte(SEND_RESULT);
        out.writeByte(status);
        out.writeBytes(msgId);
        Encoder.encode(out, info);
    }

    @Override
    public SendResultWrapper decode(ByteBuf in) {
        status = in.readByte();
        msgId = new byte[MessageId.LENGTH];
        in.readBytes(msgId);
        info = Decoder.decode(in);
        return this;
    }
}
