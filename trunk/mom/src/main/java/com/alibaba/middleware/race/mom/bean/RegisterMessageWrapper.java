package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.codec.Serializer;
import io.netty.buffer.ByteBuf;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class RegisterMessageWrapper implements SerializeWrapper<RegisterMessage> {
    private byte type;
    private byte[] groupId;
    private byte[] filter;

    @Override
    public RegisterMessage deserialize(Serializer serializer) {
        return new RegisterMessage()
                .type(RegisterMessage.ClientType.values()[type])
                .groupId((String) serializer.decode(this.groupId))
                .filter((String) serializer.decode(this.filter));
    }

    @Override
    public SerializeWrapper<RegisterMessage> serialize(RegisterMessage msg, Serializer serializer) {
        this.type = (byte) msg.type().ordinal();
        this.groupId = serializer.encode(msg.groupId());
        this.filter = serializer.encode(msg.filter());
        return this;
    }

    @Override
    public void encode(ByteBuf out) {
        out.writeByte(REGISTER);
        out.writeByte(type);
        Encoder.encode(out, groupId);
        Encoder.encode(out, filter);
    }

    @Override
    public SerializeWrapper<RegisterMessage> decode(ByteBuf in) {
        this.type = in.readByte();
        this.groupId = Decoder.decode(in);
        this.filter = Decoder.decode(in);
        return this;
    }
}
