package com.alibaba.middleware.race.mom;

public class ConsumeResult {
    private ConsumeStatus status = ConsumeStatus.FAIL;
    private String info;

    public void setStatus(ConsumeStatus status) {
        this.status = status;
    }

    public ConsumeStatus getStatus() {
        return status;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public String getInfo() {
        return info;
    }
}
