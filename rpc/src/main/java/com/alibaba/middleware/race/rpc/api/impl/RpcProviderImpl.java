package com.alibaba.middleware.race.rpc.api.impl;

import com.alibaba.middleware.race.rpc.api.Parameter;
import com.alibaba.middleware.race.rpc.api.RpcProvider;
import com.alibaba.middleware.race.rpc.api.codec.SerializeType;
import com.alibaba.middleware.race.rpc.api.codec.Serializer;
import com.alibaba.middleware.race.rpc.api.util.Logger;
import com.alibaba.middleware.race.rpc.model.RpcRequest;
import com.alibaba.middleware.race.rpc.model.RpcRequestWrapper;
import com.alibaba.middleware.race.rpc.model.RpcResponse;
import com.alibaba.middleware.race.rpc.model.RpcResponseWrapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Dawnwords on 2015/7/21.
 */
public class RpcProviderImpl extends RpcProvider {

    private Class<?> serviceInterface;
    private String version;
    private int timeout;
    private SerializeType serializeType;
    private Object serviceInstance;
    private Map<String/* methodName_argTypes */, Method> cachedMethods;
    private DefaultEventExecutorGroup defaultEventExecutorGroup =
            new DefaultEventExecutorGroup(Parameter.SERVER_EXECUTOR_THREADS,
                    new DefaultThreadFactory("NettyServerWorkerThread"));

    @Override
    public RpcProvider serviceInterface(Class<?> serviceInterface) {
        this.serviceInterface = serviceInterface;
        Map<String, Method> cachedMethods = new HashMap<String, Method>();
        for (Method method : serviceInterface.getMethods()) {
            cachedMethods.put(methodKey(method.getName(), method.getParameterTypes()), method);
        }
        this.cachedMethods = Collections.unmodifiableMap(cachedMethods);
        return this;
    }

    @Override
    public RpcProvider version(String version) {
        this.version = version;
        return this;
    }

    @Override
    public RpcProvider impl(Object serviceInstance) {
        this.serviceInstance = serviceInstance;
        return this;
    }

    @Override
    public RpcProvider timeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public RpcProvider serializeType(String serializeType) {
        this.serializeType = SerializeType.valueOf(serializeType);
        return this;
    }

    private SerializeType serializeType() {
        return Parameter.SERIALIZE_TYPE;
    }

    @Override
    public void publish() {
        new ProviderServer().start();
    }

    private class ProviderServer extends Thread {

        @Override
        public void run() {
            final EventLoopGroup bossGroup = new NioEventLoopGroup(Parameter.SERVER_BOSS_THREADS, new DefaultThreadFactory("NettyBossSelector"));
            final EventLoopGroup workerGroup = new NioEventLoopGroup(
                    Parameter.SERVER_WORKER_THREADS, new DefaultThreadFactory("NettyServerSelector"));
            new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new Initializer())
                    .option(ChannelOption.SO_BACKLOG, Parameter.BACKLOG_SIZE)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.SO_SNDBUF, Parameter.SND_BUF_SIZE)
                    .option(ChannelOption.SO_RCVBUF, Parameter.RCV_BUF_SIZE)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .bind(Parameter.SERVER_PORT)
                    .addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture channelFuture) throws Exception {
                            channelFuture.channel().closeFuture().addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                                    workerGroup.shutdownGracefully();
                                    bossGroup.shutdownGracefully();
                                    defaultEventExecutorGroup.shutdownGracefully();
                                }
                            });
                        }
                    });
        }
    }

    @ChannelHandler.Sharable
    private class Initializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            Serializer serializer = serializeType().serializer();
            pipeline.addLast(defaultEventExecutorGroup, "decoder", serializer.decoder());
            pipeline.addLast(defaultEventExecutorGroup, "encoder", serializer.encoder());
            pipeline.addLast(defaultEventExecutorGroup, "handler", new ServerRpcHandler());
        }
    }

    @ChannelHandler.Sharable
    class ServerRpcHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof List) {
                for (Object o : (List) msg) {
                    ctx.channel().eventLoop().submit(new RequestWorker(ctx, (RpcRequestWrapper) o));
                }
            } else if (msg instanceof RpcRequestWrapper) {
                ctx.channel().eventLoop().submit(new RequestWorker(ctx, (RpcRequestWrapper) msg));
            } else {
                Logger.error("[unknown request type]");
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            if (cause instanceof IOException) {
                Logger.error("[client disconnected]");
            } else {
                Logger.error(cause);
            }
            ctx.close();
        }
    }

    class RequestWorker implements Runnable {

        private ChannelHandlerContext ctx;
        private RpcRequestWrapper requestWrapper;
        private long start;

        public RequestWorker(ChannelHandlerContext ctx, RpcRequestWrapper requestWrapper) {
            this.ctx = ctx;
            this.requestWrapper = requestWrapper;
            this.start = System.currentTimeMillis();
        }

        @Override
        public void run() {
            RpcRequest request = requestWrapper.deserialize(serializeType().serializer());
            Logger.info("[receive request] %s", request);
            RpcResponse response = new RpcResponse().id(request.id());
            request.restoreContext();

            if (version != null && !version.equals(request.version())) {
                response.exception(new IllegalStateException(String.format("version not match: provided: %s, given :%s", version, request.version())));
            } else {
                try {
                    Method method = cachedMethods.get(methodKey(request.methodName(), request.parameterTypes()));
                    response.appResponse(method.invoke(serviceInstance, request.arguments()));
                } catch (InvocationTargetException e) {
                    response.exception(e.getTargetException());
                } catch (Exception e) {
                    response.exception(e);
                }
            }
            boolean notTimeout = System.currentTimeMillis() - start < timeout;
            if (notTimeout) {
                Logger.info("[send response] %s", response);
                RpcResponseWrapper wrapper = new RpcResponseWrapper();
                ctx.writeAndFlush(wrapper.serialize(response, serializeType().serializer()))
                        .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            }
        }
    }

    private String methodKey(String name, Class[] paramTypes) {
        String key = name;
        if (paramTypes != null) {
            for (Class c : paramTypes) {
                key += c.getName();
            }
        }
        return key;
    }
}
