package com.alibaba.middleware.race.mom;

import com.alibaba.middleware.race.mom.bean.MessageId;

public class ConsumeResult {
    private ConsumeStatus status;
    private MessageId msgId;
    private String info;

    public static ConsumeResult success(MessageId msgId) {
        ConsumeResult result = new ConsumeResult();
        result.status = ConsumeStatus.SUCCESS;
        result.msgId = msgId;
        return result;
    }

    public static ConsumeResult fail(MessageId msgId, String info) {
        ConsumeResult result = new ConsumeResult();
        result.status = ConsumeStatus.FAIL;
        result.msgId = msgId;
        result.info = info;
        return result;
    }

    public ConsumeStatus getStatus() {
        return status;
    }

    public void setStatus(ConsumeStatus status) {
        this.status = status;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public MessageId msgId() {
        return msgId;
    }

    public ConsumeResult msgId(MessageId msgId) {
        this.msgId = msgId;
        return this;
    }
}
