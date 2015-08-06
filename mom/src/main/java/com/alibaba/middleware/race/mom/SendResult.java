package com.alibaba.middleware.race.mom;

public class SendResult {
    private SendStatus status;
    private String msgId;
    private String info;

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
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    @Override
    public String toString() {
        return "msg " + msgId + "  send " + (status == SendStatus.SUCCESS ? "success" : "fail") + "   info:" + info;
    }

}
