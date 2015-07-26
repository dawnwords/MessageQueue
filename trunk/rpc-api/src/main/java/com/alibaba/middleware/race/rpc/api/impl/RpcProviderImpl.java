package com.alibaba.middleware.race.rpc.api.impl;

import com.alibaba.middleware.race.rpc.api.Parameter;
import com.alibaba.middleware.race.rpc.api.RpcProvider;
import com.alibaba.middleware.race.rpc.api.codec.SerializeType;
import com.alibaba.middleware.race.rpc.api.netty.ServerRpcHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

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
                    .childOption(ChannelOption.SO_KEEPALIVE, false)
                    .childOption(ChannelOption.TCP_NODELAY, true)
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
            pipeline.addLast(defaultEventExecutorGroup, "handler", new ServerRpcHandler(serviceInterface, serviceInstance, version));
        }
    }
}
