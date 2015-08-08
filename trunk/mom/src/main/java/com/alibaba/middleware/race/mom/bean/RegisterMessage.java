package com.alibaba.middleware.race.mom.bean;

import java.io.Serializable;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class RegisterMessage implements Serializable {
    private ClientType type;
    private String groupId;
    private String topic;
    private String filter;

    public ClientType type() {
        return type;
    }

    public RegisterMessage type(ClientType type) {
        this.type = type;
        return this;
    }

    public String groupId() {
        return groupId;
    }

    public RegisterMessage groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public String topic() {
        return topic;
    }

    public RegisterMessage topic(String topic) {
        this.topic = topic;
        return this;
    }

    public String filter() {
        return filter;
    }

    public RegisterMessage filter(String filter) {
        this.filter = filter;
        return this;
    }

    public enum ClientType {
        Producer, Consumer
    }

    @Override
    public String toString() {
        return "RegisterMessage{" +
                "type=" + type +
                ", groupId='" + groupId + '\'' +
                ", topic='" + topic + '\'' +
                ", filter='" + filter + '\'' +
                '}';
    }
}
