package com.alibaba.middleware.race.mom.bean;

import io.netty.buffer.ByteBuf;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class RegisterMessageWrapper implements SerializeWrapper<RegisterMessage> {
    private byte type;
    private byte[] groupId;
    private byte[] topic;
    private byte[] filter;

    @Override
    public RegisterMessage deserialize() {
        return new RegisterMessage()
                .type(RegisterMessage.ClientType.values()[type])
                .groupId(Bytes.toString(this.groupId))
                .topic(Bytes.toString(this.topic))
                .filter(Bytes.toString(this.filter));
    }

    @Override
    public RegisterMessageWrapper serialize(RegisterMessage msg) {
        this.type = (byte) msg.type().ordinal();
        this.groupId = Bytes.toBytes(msg.groupId());
        this.topic = Bytes.toBytes(msg.topic());
        this.filter = Bytes.toBytes(msg.filter());
        return this;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeByte(REGISTER);
        out.writeByte(type);
        Encoder.encode(out, groupId);
        Encoder.encode(out, topic);
        Encoder.encode(out, filter);
    }

    @Override
    public RegisterMessageWrapper decode(ByteBuf in) {
        this.type = in.readByte();
        this.groupId = Decoder.decode(in);
        this.topic = Decoder.decode(in);
        this.filter = Decoder.decode(in);
        return this;
    }
}
