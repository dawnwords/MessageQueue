package com.alibaba.middleware.race.rpc.model;

import com.alibaba.middleware.race.rpc.context.RpcContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by huangsheng.hs on 2015/5/7.
 */
public class RpcRequest implements Serializable {
    private final long id;
    private String methodName;
    private String version;
    private Object[] arguments;
    private Map<String, Object> props;

    public RpcRequest() {
        props = RpcContext.getProps();
        this.id = System.nanoTime();
    }

    public long id() {
        return id;
    }

    public RpcRequest methodName(String methodName) {
        this.methodName = methodName;
        return this;
    }

    public RpcRequest arguments(Object[] arguments) {
        this.arguments = arguments;
        return this;
    }

    public RpcRequest version(String version) {
        this.version = version;
        return this;
    }

    public String methodName() {
        return methodName;
    }

    public String version() {
        return version;
    }

    public Class<?>[] parameterTypes() {
        if (arguments == null) return null;
        Class<?>[] result = new Class<?>[arguments.length];
        int i = 0;
        for (Object each : arguments) {
            result[i] = each.getClass();
            i++;
        }
        return result;
    }

    public Object[] arguments() {
        return arguments;
    }

    public RpcRequest restoreContext() {
        RpcContext.props = props;
        return this;
    }

    @Override
    public String toString() {
        return "RpcRequest{" +
                "\n methodName='" + methodName + '\'' +
                "\n version='" + version + '\'' +
                "\n arguments=" + Arrays.toString(arguments) +
                "\n}";
    }
}
