package com.alibaba.middleware.race.mom.broker.bean;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.broker.codec.Serializer;

import java.util.Map;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class MessageWrapper implements SerializeWrapper<Message> {
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
        result.setMsgId((String) serializer.decode(msgId));
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
        this.msgId = serializer.encode(msg.getMsgId());
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
}
