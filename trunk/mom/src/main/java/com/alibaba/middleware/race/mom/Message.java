package com.alibaba.middleware.race.mom;

import com.alibaba.middleware.race.mom.bean.MessageId;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable {
    private String topic;
    private byte[] body;
    private MessageId msgId;   //全局唯一的消息id，不同消息不能重复
    private long bornTime;
    private Map<String, String> properties = new HashMap<String, String>();

    public String getMsgId() {
        return msgId.toString();
    }

    public void setMsgId(InetSocketAddress address) {
        this.bornTime = System.currentTimeMillis();
        this.msgId = new MessageId(address, bornTime);
    }

    public void setMsgId(MessageId msgId) {
        this.msgId = msgId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getProperty(String key) {
        return properties.get(key);
    }

    public void setProperty(String key, String value) {
        properties.put(key, value);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void removeProperty(String key) {
        properties.remove(key);
    }

    public long getBornTime() {
        return bornTime;
    }

    public void setBornTime(long bornTime) {
        this.bornTime = bornTime;
    }

    @Override
    public String toString() {
        return "Message{" +
                "topic='" + topic + '\'' +
                ", msgId='" + msgId + '\'' +
                ", bornTime=" + bornTime +
                ", properties=" + properties +
                '}';
    }


    public MessageId getMessageId() {
        return msgId;
    }
}
