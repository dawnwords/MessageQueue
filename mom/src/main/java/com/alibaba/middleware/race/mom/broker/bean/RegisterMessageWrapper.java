package com.alibaba.middleware.race.mom.broker.bean;

import com.alibaba.middleware.race.mom.broker.codec.Serializer;

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
}
