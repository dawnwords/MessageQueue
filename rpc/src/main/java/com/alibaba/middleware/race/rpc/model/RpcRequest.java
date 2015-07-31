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
    private static final AtomicLong ID_GEN = new AtomicLong(0);
    private long id;
    private String methodName;
    private String version;
    private Object[] arguments;
    private Map<String, Object> context;

    public RpcRequest init() {
        this.id = ID_GEN.getAndIncrement();
        return this;
    }

    public long id() {
        return id;
    }

    RpcRequest id(long id) {
        this.id = id;
        return this;
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

    Map<String, Object> context() {
        return context;
    }

    RpcRequest context(Map<String, Object> contexts) {
        this.context = contexts;
        return this;
    }

    public RpcRequest saveContext() {
        this.context = RpcContext.getProps();
        return this;
    }

    public RpcRequest restoreContext() {
        RpcContext.setProp(context);
        return this;
    }

    @Override
    public String toString() {
        return "RpcRequest{" +
                "\n methodName='" + methodName() + '\'' +
                "\n version='" + version() + '\'' +
                "\n arguments=" + Arrays.toString(arguments) +
                "\n}";
    }
}
