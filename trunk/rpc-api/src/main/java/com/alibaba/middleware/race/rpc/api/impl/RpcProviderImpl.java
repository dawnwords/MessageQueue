package com.alibaba.middleware.race.rpc.api.impl;

import com.alibaba.middleware.race.rpc.api.Parameter;
import com.alibaba.middleware.race.rpc.api.RpcProvider;
import com.alibaba.middleware.race.rpc.api.codec.SerializeType;
import com.alibaba.middleware.race.rpc.api.util.Logger;
import com.alibaba.middleware.race.rpc.model.RpcRequest;
import com.alibaba.middleware.race.rpc.model.RpcResponse;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

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
            new DefaultEventExecutorGroup(Parameter.SERVER_WORKER_THREADS,
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

    @Override
    public void publish() {
        new ProviderServer().start();
    }

    private class ProviderServer extends Thread {

        @Override
        public void run() {
            final EventLoopGroup bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("NettyBossSelector"));
            final EventLoopGroup workerGroup = new NioEventLoopGroup(
                    Parameter.SERVER_SELECTOR_THREADS, new DefaultThreadFactory("NettyServerSelector"));
            new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new Initializer())
                    .option(ChannelOption.SO_BACKLOG, Parameter.BACKLOG_SIZE)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_SNDBUF, Parameter.SND_BUF_SIZE)
                    .option(ChannelOption.SO_RCVBUF, Parameter.RCV_BUF_SIZE)
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
            pipeline.addLast(defaultEventExecutorGroup, "timeout", new ReadTimeoutHandler(timeout, TimeUnit.MILLISECONDS));
            pipeline.addLast(defaultEventExecutorGroup, "decoder", serializeType.deserializer());
            pipeline.addLast(defaultEventExecutorGroup, "encoder", serializeType.serializer());
            pipeline.addLast(defaultEventExecutorGroup, "handler", new ServerRpcHandler());
        }
    }

    @ChannelHandler.Sharable
    public class ServerRpcHandler extends SimpleChannelInboundHandler<RpcRequest> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RpcRequest request) throws Exception {
            request.restoreContext();
            Logger.info("[receive request]" + request);
            RpcResponse response = new RpcResponse();

            if (version != null && !version.equals(request.version())) {
                response.exception(new IllegalStateException(String.format("version not match: provided: %s, given :%s", version, request.version())));
            } else {
                try {
                    Method method = serviceInterface.getDeclaredMethod(request.methodName(), request.parameterTypes());
                    response.appResponse((Serializable) method.invoke(serviceInstance, request.arguments()));
                } catch (InvocationTargetException e) {
                    response.exception(e.getTargetException());
                } catch (Exception e) {
                    response.exception(e);
                }
            }
            writeResponse(ctx, response);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            writeResponse(ctx, new RpcResponse().exception(cause));
        }

        private void writeResponse(ChannelHandlerContext ctx, RpcResponse response) {
            Logger.info("[send response]" + response);
            ctx.writeAndFlush(response);
            ctx.close();
        }
    }
}
