package com.alibaba.middleware.race.rpc.api.impl;

import com.alibaba.middleware.race.rpc.aop.ConsumerHook;
import com.alibaba.middleware.race.rpc.api.Parameter;
import com.alibaba.middleware.race.rpc.api.RpcConsumer;
import com.alibaba.middleware.race.rpc.api.netty.ClientRpcHandler;
import com.alibaba.middleware.race.rpc.api.netty.ClientTimeoutHandler;
import com.alibaba.middleware.race.rpc.api.codec.SerializeType;
import com.alibaba.middleware.race.rpc.async.ResponseCallbackListener;
import com.alibaba.middleware.race.rpc.async.ResponseFuture;
import com.alibaba.middleware.race.rpc.model.RpcRequest;
import com.alibaba.middleware.race.rpc.model.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.Serializable;
import java.lang.reflect.Method;
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
    public void asynCall(String methodName) {
        asynCall(methodName, new SynchroCallbackListener());
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

            RpcResponse response = null;
            ResponseCallbackListener listener = asynMap.get(rpcRequest.methodName());
            if (listener == null) {
                listener = new SynchroCallbackListener();
            }

            sendRpcRequest(rpcRequest, listener);

            if (listener instanceof SynchroCallbackListener) {
                response = ((SynchroCallbackListener) listener).rpcResponse();
            }
            return response;
        }

    }

    private void sendRpcRequest(final RpcRequest request, ResponseCallbackListener listener) {
        final EventLoopGroup group = new NioEventLoopGroup(1, new DefaultThreadFactory("NettyClientSelector"));
        new Bootstrap().group(group).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .handler(new Initializer(request, listener))
                .connect(System.getProperty("SIP", Parameter.SERVER_IP), Parameter.SERVER_PORT)
                .addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        future.channel().writeAndFlush(request).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                future.channel().closeFuture().addListener(new ChannelFutureListener() {
                                    @Override
                                    public void operationComplete(ChannelFuture channelFuture) throws Exception {
                                        group.shutdownGracefully();
                                    }
                                });
                            }
                        });
                    }
                });
    }

    @ChannelHandler.Sharable
    private class Initializer extends ChannelInitializer<SocketChannel> {

        private final RpcRequest request;
        private final ResponseCallbackListener listener;

        public Initializer(RpcRequest request, ResponseCallbackListener listener) {
            this.request = request;
            this.listener = listener;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("timeout", new ClientTimeoutHandler(clientTimeout, listener));
            pipeline.addLast("decoder", serializeType.deserializer());
            pipeline.addLast("encoder", serializeType.serializer());
            pipeline.addLast("handler", new ClientRpcHandler(listener, hook, request));
        }
    }

    private class SynchroCallbackListener implements ResponseCallbackListener {

        private CountDownLatch latch = new CountDownLatch(1);
        private RpcResponse response = new RpcResponse();

        @Override
        public void onResponse(Object response) {
            this.response.appResponse((Serializable) response);
            latch.countDown();
        }

        @Override
        public void onTimeout() {
            latch.countDown();
        }

        @Override
        public void onException(Exception e) {
            this.response.exception(e);
            latch.countDown();
        }

        public RpcResponse rpcResponse() {
            try {
                latch.await(clientTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
            }
            return response;
        }
    }

}
