package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.SendResult;
import com.alibaba.middleware.race.mom.SendStatus;
import com.alibaba.middleware.race.mom.codec.Serializer;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class SendResultWrapper implements SerializeWrapper<SendResult> {
    private byte status;
    private byte[] msgId;
    private byte[] info;

    @Override
    public SendResult deserialize(Serializer serializer) {
        SendResult result = new SendResult();
        result.setStatus(SendStatus.values()[status]);
        result.setMsgId((String) serializer.decode(msgId));
        result.setInfo((String) serializer.decode(info));
        return result;
    }

    @Override
    public SerializeWrapper<SendResult> serialize(SendResult sendResult, Serializer serializer) {
        this.status = (byte) sendResult.getStatus().ordinal();
        this.msgId = serializer.encode(sendResult.getMsgId());
        this.info = serializer.encode(sendResult.getInfo());
        return this;
    }
}