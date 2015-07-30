package com.alibaba.middleware.race.rpc.model;

import com.alibaba.middleware.race.rpc.context.RpcContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by huangsheng.hs on 2015/5/7.
 */
public class RpcRequest implements Serializable {
    private static final AtomicLong ID_GEN = new AtomicLong(0);
    private long id;
    private byte[] methodName;
    private byte[] version;
    private Object[] arguments;
    private byte[][] propKeys;
    private Object[] propVals;

    public RpcRequest init() {
        this.id = ID_GEN.getAndIncrement();
        return this;
    }

    public long id() {
        return id;
    }

    public RpcRequest methodName(String methodName) {
        this.methodName = methodName.getBytes();
        return this;
    }

    public RpcRequest arguments(Object[] arguments) {
        this.arguments = arguments;
        return this;
    }

    public RpcRequest version(String version) {
        this.version = version.getBytes();
        return this;
    }

    public String methodName() {
        return new String(methodName);
    }

    public String version() {
        return new String(version);
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

    public RpcRequest saveContext() {
        Map<String, Object> props = RpcContext.getProps();
        ArrayList<byte[]> propKeys = new ArrayList<byte[]>();
        ArrayList<Object> propVals = new ArrayList<Object>();
        for (String key : props.keySet()) {
            propKeys.add(key.getBytes());
            propVals.add(props.get(key));
        }
        int size = propKeys.size();
        this.propKeys = propKeys.toArray(new byte[size][]);
        this.propVals = propVals.toArray(new Object[size]);
        return this;
    }

    public RpcRequest restoreContext() {
        for (int i = 0; i < propKeys.length; i++) {
            RpcContext.addProp(new String(propKeys[i]), propVals[i]);
        }
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
