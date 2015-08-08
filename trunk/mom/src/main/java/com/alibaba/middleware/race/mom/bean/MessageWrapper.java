package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.codec.Serializer;
import com.alibaba.middleware.race.mom.store.Storable;
import com.alibaba.middleware.race.mom.util.MessageIdUtil;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class MessageWrapper implements SerializeWrapper<Message>, Storable<SerializeWrapper<Message>> {
    private byte[] topic;
    private byte[] body;
    private byte[] msgId;
    private long bornTime;
    private byte[][] propKeys;
    private byte[][] propVals;

    @Override
    public Message deserialize(Serializer serializer) {
        Message result = new Message();
        result.setTopic((String) serializer.decode(topic));
        result.setBody(body);
        result.setMsgId(msgId);
        result.setBornTime(bornTime);
        for (int i = 0; i < propKeys.length; i++) {
            result.setProperty((String) serializer.decode(propKeys[i]), (String) serializer.decode(propVals[i]));
        }
        return result;
    }

    @Override
    public SerializeWrapper<Message> serialize(Message msg, Serializer serializer) {
        this.topic = serializer.encode(msg.getTopic());
        this.body = msg.getBody();
        this.msgId = msg.getMsgIdAsByte();
        this.bornTime = msg.getBornTime();
        Map<String, String> properties = msg.getProperties();
        this.propKeys = new byte[properties.size()][];
        this.propVals = new byte[properties.size()][];
        int i = 0;
        for (String key : properties.keySet()) {
            propKeys[i] = serializer.encode(key);
            propVals[i] = serializer.encode(properties.get(key));
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
    public SerializeWrapper<Message> decode(ByteBuf in) {
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
        int totalLen = 8;   //bornTime.length
        totalLen += 4 + topic.length;
        totalLen += 4 + body.length;
        totalLen += 4;      // propKey.size
        for (int i = 0; i < propKeys.length; i++) {
            totalLen += 4 + propKeys[i].length;
            totalLen += 4 + propVals[i].length;
        }
        ByteBuffer result = ByteBuffer.allocate(totalLen);
        result.putLong(bornTime);
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
    public SerializeWrapper<Message> fromStorage(byte[] id, byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        this.bornTime = buffer.getLong();
        this.topic = get(buffer);
        this.body = get(buffer);
        int propertiesLen = buffer.getInt();
        this.propKeys = new byte[propertiesLen][];
        this.propVals = new byte[propertiesLen][];
        for (int i = 0; i < propertiesLen; i++) {
            this.propKeys[i] = get(buffer);
            this.propVals[i] = get(buffer);
        }
        this.msgId = id;
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
                "msgId=" + MessageIdUtil.toString(msgId) +
                '}';
    }

    public byte[] msgId() {
        return msgId;
    }
}
