package com.alibaba.middleware.race.rpc.api.impl;

import com.alibaba.middleware.race.rpc.api.Parameter;
import com.alibaba.middleware.race.rpc.api.RpcProvider;
import com.alibaba.middleware.race.rpc.api.codec.SerializeType;
import com.alibaba.middleware.race.rpc.api.util.Logger;
import com.alibaba.middleware.race.rpc.model.RpcRequest;
import com.alibaba.middleware.race.rpc.model.RpcResponse;
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

/**
 * Created by Dawnwords on 2015/7/21.
 */
public class RpcProviderImpl extends RpcProvider {

    private Class<?> serviceInterface;
    private String version;
    private int timeout;
    private SerializeType serializeType;
    private Object serviceInstance;
    private DefaultEventExecutorGroup defaultEventExecutorGroup =
            new DefaultEventExecutorGroup(Parameter.SERVER_EXECUTOR_THREADS,
                    new DefaultThreadFactory("NettyServerWorkerThread"));

    @Override
    public RpcProvider serviceInterface(Class<?> serviceInterface) {
        this.serviceInterface = serviceInterface;
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
            pipeline.addLast(defaultEventExecutorGroup, "decoder", serializeType().deserializer());
            pipeline.addLast(defaultEventExecutorGroup, "encoder", serializeType().serializer());
            pipeline.addLast(defaultEventExecutorGroup, "handler", new ServerRpcHandler());
        }
    }

    @ChannelHandler.Sharable
    class ServerRpcHandler extends SimpleChannelInboundHandler<RpcRequest> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final RpcRequest request) {
            Logger.info("[receive request]" + request);
            final long start = System.currentTimeMillis();
            ctx.channel().eventLoop().submit(new Runnable() {
                @Override
                public void run() {
                    request.restoreContext();
                    RpcResponse response = new RpcResponse().id(request.id());

                    if (version != null && !version.equals(request.version())) {
                        response.exception(new IllegalStateException(String.format("version not match: provided: %s, given :%s", version, request.version())));
                    } else {
                        try {
                            Method method = serviceInterface.getDeclaredMethod(request.methodName(), request.parameterTypes());
                            response.appResponse(method.invoke(serviceInstance, request.arguments()));
                        } catch (InvocationTargetException e) {
                            response.exception(e.getTargetException());
                        } catch (Exception e) {
                            response.exception(e);
                        }
                    }
                    boolean notTimeout = System.currentTimeMillis() - start < timeout;
                    if (notTimeout) {
                        Logger.info("[send response]" + response);
                        ctx.writeAndFlush(response).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
                    }
                }
            });
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            if (cause instanceof IOException) {
                Logger.error("[client disconnected]");
            }
            ctx.close();
        }
    }
}
