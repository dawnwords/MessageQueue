package com.alibaba.middleware.race.rpc.api.impl;

import com.alibaba.middleware.race.rpc.aop.ConsumerHook;
import com.alibaba.middleware.race.rpc.api.Parameter;
import com.alibaba.middleware.race.rpc.api.RpcConsumer;
import com.alibaba.middleware.race.rpc.api.serializer.SerializeType;
import com.alibaba.middleware.race.rpc.async.ResponseCallbackListener;
import com.alibaba.middleware.race.rpc.async.ResponseFuture;
import com.alibaba.middleware.race.rpc.model.RpcRequest;
import com.alibaba.middleware.race.rpc.model.RpcResponse;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.concurrent.*;

/**
 * Created by Dawnwords on 2015/7/21.
 */
public class RpcConsumerImpl extends RpcConsumer {
    private SerializeType serializeType;
    private String version;
    private int clientTimeout;
    private ConsumerHook hook;
    private ConcurrentHashMap<String, ResponseCallbackListener> asynMap;
    private ExecutorService threadPool;

    public RpcConsumerImpl() {
        super();
        this.serializeType = SerializeType.java;
        this.asynMap = new ConcurrentHashMap<>();
        this.threadPool = Executors.newCachedThreadPool();
    }

    @Override
    public RpcConsumer version(String version) {
        if (version == null) {
            throw new NullPointerException("version is null");
        }
        this.version = version;
        return this;
    }

    @Override
    public RpcConsumer clientTimeout(int clientTimeout) {
        this.clientTimeout = clientTimeout;
        return this;
    }

    @Override
    public RpcConsumer hook(ConsumerHook hook) {
        this.hook = hook;
        return this;
    }

    @Override
    public <T extends ResponseCallbackListener> void asynCall(String methodName, T callbackListener) {
        if (methodName == null) {
            throw new NullPointerException("methodName is null");
        }
        asynMap.put(methodName, callbackListener);
    }

    @Override
    public void cancelAsyn(String methodName) {
        asynMap.remove(methodName);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        RpcRequest rpcRequest = new RpcRequest()
                .version(version)
                .methodName(method.getName())
                .parameterTypes(method.getParameterTypes())
                .arguments(args);

        ResponseFuture.setFuture(threadPool.submit(new RpcCallable(rpcRequest)));
        if (!asynMap.containsKey(rpcRequest.methodName())) {
            return ResponseFuture.getResponse(clientTimeout);
        }
        return null;
    }

    private static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignored) {
            }
        }
    }

    private class RpcCallable implements Callable<Object> {
        private RpcRequest rpcRequest;

        public RpcCallable(RpcRequest rpcRequest) {
            this.rpcRequest = rpcRequest;
        }

        @Override
        public Object call() throws Exception {
            if (hook != null) {
                hook.before(rpcRequest);
            }

            Socket socket = null;
            RpcResponse response = null;
            ResponseCallbackListener listener = asynMap.get(rpcRequest.methodName());
            try {
                //TODO type & null check
                socket = new Socket(Parameter.SERVER_IP, Parameter.SERVER_PORT);
                socket.setSoTimeout(clientTimeout);
                serializeType.serialize(rpcRequest, socket.getOutputStream());
                response = (RpcResponse) serializeType.deserialize(socket.getInputStream());
            } catch (Exception e) {
                if (listener != null) {
                    if (e.getMessage().contains("Time out")) {
                        listener.onTimeout();
                    } else {
                        listener.onException(e);
                    }
                }
            } finally {
                close(socket);
            }

            if (hook != null) {
                hook.after(rpcRequest);
            }

            if (listener == null || listener.equals(ResponseCallbackListener.NULL)) {
                return response;
            }

            if (response != null) {
                if (response.hasException()) {
                    listener.onException(response.exception());
                } else {
                    listener.onResponse(response.appResponse());
                }
            }
            return null;
        }
    }
}
