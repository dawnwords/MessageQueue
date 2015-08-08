package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.util.ByteUtil;
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
                .groupId(ByteUtil.toString(this.groupId))
                .topic(ByteUtil.toString(this.topic))
                .filter(ByteUtil.toString(this.filter));
    }

    @Override
    public SerializeWrapper<RegisterMessage> serialize(RegisterMessage msg) {
        this.type = (byte) msg.type().ordinal();
        this.groupId = ByteUtil.toBytes(msg.groupId());
        this.topic = ByteUtil.toBytes(msg.topic());
        this.filter = ByteUtil.toBytes(msg.filter());
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
    public SerializeWrapper<RegisterMessage> decode(ByteBuf in) {
        this.type = in.readByte();
        this.groupId = Decoder.decode(in);
        this.topic = Decoder.decode(in);
        this.filter = Decoder.decode(in);
        return this;
    }
}