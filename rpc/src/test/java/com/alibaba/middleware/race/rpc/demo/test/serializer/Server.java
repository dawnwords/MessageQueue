package com.alibaba.middleware.race.rpc.demo.test.serializer;

import com.alibaba.middleware.race.rpc.api.Parameter;
import com.alibaba.middleware.race.rpc.api.codec.SerializeType;
import com.alibaba.middleware.race.rpc.api.util.Logger;
import com.alibaba.middleware.race.rpc.demo.service.RaceDO;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;

/**
 * Created by Dawnwords on 2015/7/21.
 */
public class Server extends Thread {
    private SerializeType serializeType;
    private DefaultEventExecutorGroup defaultEventExecutorGroup =
            new DefaultEventExecutorGroup(Parameter.SERVER_EXECUTOR_THREADS,
                    new DefaultThreadFactory("NettyServerWorkerThread"));

    public Server(SerializeType serializeType) {
        this.serializeType = serializeType;
    }

    @Override
    public void run() {
        final EventLoopGroup bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("NettyBossSelector"));
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
                .syncUninterruptibly();
    }

    @ChannelHandler.Sharable
    private class Initializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(defaultEventExecutorGroup, "decoder", serializeType.deserializer());
            pipeline.addLast(defaultEventExecutorGroup, "encoder", serializeType.serializer());
            pipeline.addLast(defaultEventExecutorGroup, "handler", new ServerRpcHandler());
        }
    }

    @ChannelHandler.Sharable
    public class ServerRpcHandler extends SimpleChannelInboundHandler<RaceDO> {

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final RaceDO raceDO) throws Exception {
            Logger.info("[receive]" + raceDO);
            ctx.channel().eventLoop().submit(new Runnable() {
                @Override
                public void run() {
                    Logger.info("[send]" + raceDO);
                    ctx.writeAndFlush(raceDO);
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (cause instanceof IOException) {
                Logger.error("[client disconnected]");
                ctx.close();
                System.exit(0);
            }
        }
    }

    public static void main(String[] args) {
        new Server(TestSerializeType.serializeType()).start();
    }
}
