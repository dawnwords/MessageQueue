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
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
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
    private ConcurrentHashMap<Long/* requestId */, BlockingQueue<RpcResponse>> responseMap;
    private ConcurrentHashMap<Long/* requestId */, ResponseCallbackListener> responseCallbackMap;
    private ExecutorService threadPool;
    private Channel channel;

    public RpcConsumerImpl() {
        this.serializeType = Parameter.SERIALIZE_TYPE;
        this.asynMethodCallbackMap = new ConcurrentHashMap<String, ResponseCallbackListener>();
        this.responseCallbackMap = new ConcurrentHashMap<Long, ResponseCallbackListener>();
        this.responseMap = new ConcurrentHashMap<Long, BlockingQueue<RpcResponse>>();
        this.threadPool = Executors.newCachedThreadPool();
        this.channel = new Bootstrap()
                .group(new NioEventLoopGroup(Parameter.CLIENT_THREADS, new DefaultThreadFactory("NettyClientSelector")))
                .channel(NioSocketChannel.class)
                .handler(new Initializer())
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_SNDBUF, Parameter.SND_BUF_SIZE)
                .option(ChannelOption.SO_RCVBUF, Parameter.RCV_BUF_SIZE)
                .connect(System.getProperty("SIP", Parameter.SERVER_IP), Parameter.SERVER_PORT)
                .syncUninterruptibly()
                .channel();
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
        asynCall(methodName, ResponseCallbackListener.NULL);
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
        final RpcRequest rpcRequest = new RpcRequest()
                .init()
                .version(version)
                .methodName(method.getName())
                .arguments(args);

        final ResponseCallbackListener listener = asynMethodCallbackMap.get(rpcRequest.methodName());
        boolean isSynchronous = listener == null;
        if (isSynchronous) {
            RpcResponse response = synchroCall(rpcRequest);
            if (response == null) {
                throw new TimeoutException("rpc time out");
            }
            if (response.hasException()) {
                throw response.exception();
            }
            return response.appResponse();
        }

        boolean callback = listener != ResponseCallbackListener.NULL;
        if (callback) {
            sendRequest(rpcRequest);
            responseCallbackMap.put(rpcRequest.id(), new HookedCallbackListener(rpcRequest, listener));
        } else {
            ResponseFuture.setFuture(threadPool.submit(new Callable<Object>() {
                @Override
                public RpcResponse call() throws Exception {
                    return synchroCall(rpcRequest);
                }
            }));
        }
        return null;
    }

    private RpcResponse synchroCall(RpcRequest rpcRequest) throws InterruptedException {
        responseMap.put(rpcRequest.id(), new LinkedBlockingQueue<RpcResponse>(1));
        sendRequest(rpcRequest);
        RpcResponse response = responseMap.get(rpcRequest.id()).poll(clientTimeout, TimeUnit.MILLISECONDS);
        if (hook != null) {
            hook.after(rpcRequest);
        }
        return response;
    }

    private void sendRequest(final RpcRequest rpcRequest) {
        if (hook != null) {
            hook.before(rpcRequest);
        }
        Logger.info("[send request]" + rpcRequest);
        channel.writeAndFlush(rpcRequest).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    return;
                }
                handleResponse(new RpcResponse().id(rpcRequest.id()).exception(future.cause()));
            }
        });
    }


    private void handleResponse(RpcResponse response) throws InterruptedException {
        Logger.info("[receive response]" + response);
        BlockingQueue<RpcResponse> responses = responseMap.get(response.id());
        boolean callback = responses == null;
        if (callback) {
            ResponseCallbackListener listener = responseCallbackMap.get(response.id());
            if (listener == null) {
                Logger.error("[unexpected id]" + response.id());
                return;
            }
            if (response.hasException()) {
                listener.onException(response.exception());
            } else {
                listener.onResponse(response.appResponse());
            }
        } else {
            responses.put(response);
        }
    }

    @ChannelHandler.Sharable
    private class Initializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("decoder", serializeType.deserializer());
            pipeline.addLast("encoder", serializeType.serializer());
            pipeline.addLast("handler", new ClientRpcHandler());
        }
    }

    @ChannelHandler.Sharable
    class ClientRpcHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof List) {
                for (RpcResponse response : (List<RpcResponse>) msg) {
                    handleResponse(response);
                }
            } else if (msg instanceof RpcResponse) {
                handleResponse((RpcResponse) msg);
            } else {
                Logger.error("[unknown response type]" + msg.getClass().getName());
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
        private Timer timer;

        public HookedCallbackListener(final RpcRequest request, ResponseCallbackListener listener) {
            this.listener = listener;
            this.request = request;
            this.timer = new Timer();
            this.timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    onTimeout();
                    responseCallbackMap.remove(request.id());
                }
            }, clientTimeout);
        }

        @Override
        public void onResponse(Object response) {
            listener.onResponse(response);
            timer.cancel();
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
            timer.cancel();
            after();
        }

        private void after() {
            if (hook != null) {
                hook.after(request);
            }
        }
    }
}
