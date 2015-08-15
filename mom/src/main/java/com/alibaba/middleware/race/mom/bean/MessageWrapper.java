package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.store.MessageState;
import com.alibaba.middleware.race.mom.store.Storable;
import com.alibaba.middleware.race.mom.store.StorageUnit;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class MessageWrapper implements SerializeWrapper<Message>, Storable<MessageWrapper> {
    private byte[] topic;
    private byte[] body;
    private byte[] msgId;
    private long bornTime;
    private byte[][] propKeys;
    private byte[][] propVals;

    @Override
    public Message deserialize() {
        Message result = new Message();
        result.setTopic(Bytes.toString(topic));
        result.setBody(body);
        result.setMsgId(new MessageId(msgId));
        result.setBornTime(bornTime);
        for (int i = 0; i < propKeys.length; i++) {
            result.setProperty(Bytes.toString(propKeys[i]), Bytes.toString(propVals[i]));
        }
        return result;
    }

    @Override
    public MessageWrapper serialize(Message msg) {
        this.topic = Bytes.toBytes(msg.getTopic());
        this.body = msg.getBody();
        this.msgId = msg.getMessageId().id();
        this.bornTime = msg.getBornTime();
        Map<String, String> properties = msg.getProperties();
        this.propKeys = new byte[properties.size()][];
        this.propVals = new byte[properties.size()][];
        int i = 0;
        for (String key : properties.keySet()) {
            propKeys[i] = Bytes.toBytes(key);
            propVals[i] = Bytes.toBytes(properties.get(key));
            i++;
        }
        return this;
    }

    @Override
    public void encode(ByteBuf out) {
        StorageUnit unit = toStorage();
        out.writeByte(MESSAGE);
        out.writeBytes(unit.msg());
    }

    @Override
    public MessageWrapper decode(ByteBuf in) {
        return fromStorage(new StorageUnit().msg(in.nioBuffer()));
    }

    @Override
    public StorageUnit toStorage() {
        int bodyLength = bodyLength();
        ByteBuffer buffer = ByteBuffer.allocate(bodyLength + StorageUnit.HEADER_LENGTH);
        buffer.put(msgId);
        buffer.putInt(bodyLength);
        buffer.putInt(MessageState.FAIL.ordinal());
        put(buffer, topic);
        buffer.putInt(propKeys.length);
        for (int i = 0; i < propKeys.length; i++) {
            put(buffer, propKeys[i]);
            put(buffer, propVals[i]);
        }
        buffer.putLong(bornTime);
        put(buffer, body);
        buffer.flip();
        return new StorageUnit().msg(buffer);
    }

    private int bodyLength() {
        int bodyLength = 4 + topic.length;
        bodyLength += 4;    // propNum
        for (int i = 0; i < propKeys.length; i++) {
            bodyLength += 4 + propKeys[i].length;
            bodyLength += 4 + propVals[i].length;
        }
        bodyLength += 8;    //born time
        bodyLength += 4 + body.length;
        return bodyLength;
    }

    @Override
    public MessageWrapper fromStorage(StorageUnit unit) {
        ByteBuffer msg = unit.msg();
        this.msgId = new byte[MessageId.LENGTH];
        msg.get(msgId);
        msg.getInt();        //ignore length
        msg.getInt();        //ignore status
        this.topic = get(msg);
        int propertiesLen = msg.getInt();
        this.propKeys = new byte[propertiesLen][];
        this.propVals = new byte[propertiesLen][];
        for (int i = 0; i < propertiesLen; i++) {
            this.propKeys[i] = get(msg);
            this.propVals[i] = get(msg);
        }
        this.bornTime = msg.getLong();
        this.body = get(msg);
        return this;
    }

    private void put(ByteBuffer buffer, byte[] bytes) {
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    private byte[] get(ByteBuffer buffer) {
        byte[] result = new byte[buffer.getInt()];
        buffer.get(result);
        return result;
    }

    @Override
    public String toString() {
        return "MessageWrapper{" +
                "msgId=" + new MessageId(msgId) +
                '}';
    }

    public MessageId msgId() {
        return new MessageId(msgId);
    }

    public String topic() {
        return Bytes.toString(topic);
    }

    public String filter() {
        if (propKeys == null || propKeys.length == 0) {
            return null;
        }
        int i = 0;
        StringBuilder builder = new StringBuilder();
        builder.append(Bytes.toString(propKeys[i]));
        builder.append("=");
        builder.append(Bytes.toString(propVals[i]));
        i++;
        for (; i < propKeys.length; i++) {
            builder.append(";");
            builder.append(Bytes.toString(propKeys[i]));
            builder.append("=");
            builder.append(Bytes.toString(propVals[i]));
        }
        return builder.toString();
    }
}
