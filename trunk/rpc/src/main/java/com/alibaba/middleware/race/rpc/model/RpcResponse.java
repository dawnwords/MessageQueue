package com.alibaba.middleware.race.rpc.model;

import java.io.Serializable;

/**
 * Created by huangsheng.hs on 2015/3/27.
 */
public class RpcResponse implements Serializable {
    private long id;
    private RuntimeException exception;
    private Object appResponse;

    public long id() {
        return id;
    }

    public RpcResponse id(long id) {
        this.id = id;
        return this;
    }

    public RuntimeException exception() {
        return exception;
    }

    public RpcResponse exception(Throwable throwable) {
        this.exception = throwable == null ? null :
                throwable instanceof RuntimeException ? (RuntimeException) throwable :
                        new RuntimeException(throwable);
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
