package com.alibaba.middleware.race.mom;

public class ConsumeResult {
    private ConsumeStatus status = ConsumeStatus.FAIL;
    private String info;

    public static ConsumeResult successResult(String info) {
        ConsumeResult result = new ConsumeResult();
        result.setStatus(ConsumeStatus.SUCCESS);
        result.setInfo(info);
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

    @Override
    public String toString() {
        return "ConsumeResult{" +
                "status=" + status +
                ", info='" + info + '\'' +
                '}';
    }
}
