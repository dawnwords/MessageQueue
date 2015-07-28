package com.alibaba.middleware.race.rpc.model;

import java.io.Serializable;

/**
 * Created by huangsheng.hs on 2015/3/27.
 */
public class RpcResponse implements Serializable {
    private RuntimeException exception;
    private Object appResponse;

    public RpcResponse exception(RuntimeException exception) {
        this.exception = exception;
        return this;
    }

    public RuntimeException exception() {
        return exception;
    }

    public RpcResponse exception(Throwable throwable) {
        this.exception = throwable instanceof RuntimeException ?
                (RuntimeException) throwable : new RuntimeException(throwable.getMessage());
        return this;
    }

    public RpcResponse appResponse(Object appResponse) {
        this.appResponse = appResponse;
        return this;
    }

    public Object appResponse() {
        return appResponse;
    }

    public boolean hasException() {
        return exception != null;
    }

    @Override
    public String toString() {
        return hasException() ? ("Exception:" + exception) : ("Response:" + appResponse);
    }
}
