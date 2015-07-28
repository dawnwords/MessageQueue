package com.alibaba.middleware.race.rpc.api.impl;

import com.alibaba.middleware.race.rpc.aop.ConsumerHook;
import com.alibaba.middleware.race.rpc.api.Parameter;
import com.alibaba.middleware.race.rpc.api.RpcConsumer;
import com.alibaba.middleware.race.rpc.api.codec.SerializeType;
import com.alibaba.middleware.race.rpc.api.util.Logger;
import com.alibaba.middleware.race.rpc.async.ResponseCallbackListener;
import com.alibaba.middleware.race.rpc.async.ResponseFuture;
import com.alibaba.middleware.race.rpc.model.RpcRequest;
import com.alibaba.middleware.race.rpc.model.RpcResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

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
    private ConcurrentHashMap<String/* methodName */, ResponseCallbackListener> asynMethodCallbackMap;
    private ExecutorService threadPool;
    private Bootstrap connector;

    public RpcConsumerImpl() {
        this.serializeType = Parameter.SERIALIZE_TYPE; //TODO use kryo
        this.asynMethodCallbackMap = new ConcurrentHashMap<>();
        this.threadPool = Executors.newCachedThreadPool();
        this.connector = new Bootstrap()
                .group(new NioEventLoopGroup(1, new DefaultThreadFactory("NettyClientSelector")))
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.SO_SNDBUF, Parameter.SND_BUF_SIZE)
                .option(ChannelOption.SO_RCVBUF, Parameter.RCV_BUF_SIZE);
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
        asynMethodCallbackMap.put(methodName, callbackListener);
    }

    @Override
    public void cancelAsyn(String methodName) {
        asynMethodCallbackMap.remove(methodName);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        RpcRequest rpcRequest = new RpcRequest()
                .version(version)
                .methodName(method.getName())
                .parameterTypes(method.getParameterTypes())
                .arguments(args);

        boolean isSynchronous = !asynMethodCallbackMap.containsKey(rpcRequest.methodName());
        ResponseFuture.setFuture(threadPool.submit(new RpcCallable(rpcRequest)));
        if (isSynchronous) {
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
            ResponseCallbackListener listener = asynMethodCallbackMap.get(rpcRequest.methodName());
            if (listener == null) {
                listener = new SynchroCallbackListener();
            }

            final HookedCallbackListener hookedListener = new HookedCallbackListener(listener, rpcRequest);
            connector.handler(new Initializer(hookedListener))
                    .connect(System.getProperty("SIP", Parameter.SERVER_IP), Parameter.SERVER_PORT)
                    .addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            future.channel().writeAndFlush(rpcRequest).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    future.channel().closeFuture();
                                }
                            });
                        }
                    });

            if (listener instanceof SynchroCallbackListener) {
                response = ((SynchroCallbackListener) listener).rpcResponse();
            }
            return response;
        }

    }


    @ChannelHandler.Sharable
    private class Initializer extends ChannelInitializer<SocketChannel> {

        private ResponseCallbackListener listener;

        public Initializer(ResponseCallbackListener listener) {
            this.listener = listener;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("timeout", new TimeoutHandler(listener));
            pipeline.addLast("decoder", serializeType.deserializer());
            pipeline.addLast("encoder", serializeType.serializer());
            pipeline.addLast("handler", new ClientRpcHandler(listener));
        }
    }

    @ChannelHandler.Sharable
    public class TimeoutHandler extends ReadTimeoutHandler {

        private ResponseCallbackListener listener;

        public TimeoutHandler(ResponseCallbackListener listener) {
            super(clientTimeout, TimeUnit.MILLISECONDS);
            this.listener = listener;
        }

        @Override
        protected void readTimedOut(ChannelHandlerContext ctx) throws Exception {
            if (listener != null) {
                listener.onTimeout();
            }
            Logger.info("[client read time out]");
            super.readTimedOut(ctx);
        }
    }

    @ChannelHandler.Sharable
    class ClientRpcHandler extends SimpleChannelInboundHandler<RpcResponse> {

        private ResponseCallbackListener listener;

        public ClientRpcHandler(ResponseCallbackListener listener) {
            this.listener = listener;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RpcResponse response) throws Exception {
            Logger.info("[receive response]" + response);
            if (listener != null) {
                if (response.hasException()) {
                    listener.onException(response.exception());
                } else {
                    listener.onResponse(response.appResponse());
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            Logger.error(cause);
        }
    }

    private class HookedCallbackListener implements ResponseCallbackListener {

        private ResponseCallbackListener listener;
        private RpcRequest request;

        public HookedCallbackListener(ResponseCallbackListener listener, RpcRequest request) {
            this.listener = listener;
            this.request = request;
        }

        @Override
        public void onResponse(Object response) {
            listener.onResponse(response);
            after();
        }

        @Override
        public void onTimeout() {
            listener.onTimeout();
            after();
        }

        @Override
        public void onException(Exception e) {
            listener.onException(e);
            after();
        }

        private void after() {
            if (hook != null) {
                hook.after(request);
            }
        }
    }

    private class SynchroCallbackListener implements ResponseCallbackListener {

        private CountDownLatch latch = new CountDownLatch(1);
        private RpcResponse response = new RpcResponse();

        @Override
        public void onResponse(Object response) {
            this.response.appResponse(response);
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
