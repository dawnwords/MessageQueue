package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.store.MessageState;
import com.alibaba.middleware.race.mom.store.Storable;
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
        out.writeByte(MESSAGE);
        Encoder.encode(out, topic);
        Encoder.encode(out, body);
        out.writeBytes(msgId);
        out.writeLong(bornTime);
        Encoder.encode(out, propKeys);
        Encoder.encode(out, propVals);
    }

    @Override
    public MessageWrapper decode(ByteBuf in) {
        this.topic = Decoder.decode(in);
        this.body = Decoder.decode(in);
        this.msgId = new byte[16];
        in.readBytes(msgId);
        this.bornTime = in.readLong();
        this.propKeys = Decoder.decodeArray(in);
        this.propVals = Decoder.decodeArray(in);
        return this;
    }

    @Override
    public byte[] toStorage() {
        int totalLen = 0;
        totalLen += 4 + topic.length;
        totalLen += 4 + body.length;
        totalLen += 4;      // propKey.size
        for (int i = 0; i < propKeys.length; i++) {
            totalLen += 4 + propKeys[i].length;
            totalLen += 4 + propVals[i].length;
        }
        ByteBuffer result = ByteBuffer.allocate(totalLen + 24/*id + status + length*/);
        result.put(msgId);
        result.putInt(MessageState.FAIL.ordinal());
        result.putInt(totalLen);
        put(result, topic);
        put(result, body);
        result.putInt(propKeys.length);
        for (int i = 0; i < propKeys.length; i++) {
            put(result, propKeys[i]);
            put(result, propVals[i]);
        }
        return result.array();
    }

    @Override
    public MessageWrapper fromStorage(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        this.msgId = new byte[16];
        buffer.get(msgId);
        buffer.getLong();   //ignore status & length
        ByteBuffer timeBuffer = ByteBuffer.wrap(msgId);
        timeBuffer.getLong();   //ignore ip & port
        this.bornTime = timeBuffer.getLong();
        this.topic = get(buffer);
        this.body = get(buffer);
        int propertiesLen = buffer.getInt();
        this.propKeys = new byte[propertiesLen][];
        this.propVals = new byte[propertiesLen][];
        for (int i = 0; i < propertiesLen; i++) {
            this.propKeys[i] = get(buffer);
            this.propVals[i] = get(buffer);
        }
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
