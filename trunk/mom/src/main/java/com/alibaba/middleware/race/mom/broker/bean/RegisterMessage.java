package com.alibaba.middleware.race.mom.broker.bean;

import java.io.Serializable;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class RegisterMessage implements Serializable {
    public enum ClientType {
        Producer, Consumer
    }

    private ClientType type;
    private String groupId;
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

    public String filter() {
        return filter;
    }

    public RegisterMessage filter(String filter) {
        this.filter = filter;
        return this;
    }
}
