package com.alibaba.middleware.race.mom;

import com.alibaba.middleware.race.mom.util.MessageIdUtil;

public class SendResult {
    private SendStatus status;
    private byte[] msgId;
    private String info;

    public static SendResult fail(byte[] msgId, String info) {
        SendResult result = new SendResult();
        result.setStatus(SendStatus.FAIL);
        result.setMsgId(msgId);
        result.setInfo(info);
        return result;
    }

    public static SendResult success(byte[] msgId) {
        SendResult result = new SendResult();
        result.setStatus(SendStatus.SUCCESS);
        result.setMsgId(msgId);
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
        return MessageIdUtil.toString(msgId);
    }

    public void setMsgId(byte[] msgId) {
        this.msgId = msgId;
    }

    @Override
    public String toString() {
        return "msg " + msgId + "  send " + (status == SendStatus.SUCCESS ? "success" : "fail") + "   info:" + info;
    }

    public byte[] getMsgIdAsArray() {
        return msgId;
    }
}
