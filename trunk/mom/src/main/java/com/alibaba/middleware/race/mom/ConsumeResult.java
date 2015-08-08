package com.alibaba.middleware.race.mom;

public class ConsumeResult {
    private ConsumeStatus status;
    private byte[] msgId;
    private String info;

    public static ConsumeResult success(byte[] msgId) {
        ConsumeResult result = new ConsumeResult();
        result.status = ConsumeStatus.SUCCESS;
        result.msgId = msgId;
        return result;
    }

    public static ConsumeResult fail(byte[] msgId, String info) {
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

    public byte[] msgId() {
        return msgId;
    }
}
