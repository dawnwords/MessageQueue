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
            EventLoopGroup bossGroup = new NioEventLoopGroup();
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            try {
                new ServerBootstrap()
                        .group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new Initializer())
                        .option(ChannelOption.SO_BACKLOG, Parameter.BACKLOG_SIZE)
                        .childOption(ChannelOption.SO_KEEPALIVE, true)
                        .childOption(ChannelOption.TCP_NODELAY, true)
                        .bind(Parameter.SERVER_PORT)
                        .sync()
                        .channel()
                        .closeFuture()
                        .sync();
            } catch (InterruptedException ignored) {
            } finally {
                workerGroup.shutdownGracefully();
                bossGroup.shutdownGracefully();
            }
        }
    }

    @ChannelHandler.Sharable
    private class Initializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("timeout", new ReadTimeoutHandler(timeout, TimeUnit.MILLISECONDS));
            pipeline.addLast("decoder", serializeType.deserializer());
            pipeline.addLast("encoder", serializeType.serializer());
            pipeline.addLast("handler", new ServerRpcHandler(serviceInterface, serviceInstance, version));
        }
    }
}
