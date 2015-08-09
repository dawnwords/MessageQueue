package com.alibaba.middleware.race.mom;

import com.alibaba.middleware.race.mom.bean.MessageId;

public class SendResult {
    private SendStatus status;
    private MessageId msgId;
    private String info;

    public static SendResult fail(MessageId msgId, String info) {
        SendResult result = new SendResult();
        result.status = SendStatus.FAIL;
        result.msgId = msgId;
        result.info = info;
        return result;
    }

    public static SendResult success(MessageId msgId) {
        SendResult result = new SendResult();
        result.status = SendStatus.SUCCESS;
        result.msgId = msgId;
        return result;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public SendStatus getStatus() {
        return status;
    }

    public void setStatus(SendStatus status) {
        this.status = status;
    }

    public String getMsgId() {
        return msgId.toString();
    }

    public void setMsgId(byte[] msgId) {
        this.msgId = new MessageId(msgId);
    }

    @Override
    public String toString() {
        return "SendResult{" +
                "status=" + status +
                ", msgId=" + msgId +
                ", info='" + info + '\'' +
                '}';
    }

    public MessageId getMessageId() {
        return msgId;
    }
}
