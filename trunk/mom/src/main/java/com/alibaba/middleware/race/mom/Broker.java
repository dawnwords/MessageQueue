package com.alibaba.middleware.race.mom;

import com.alibaba.middleware.race.mom.bean.MessageWrapper;
import com.alibaba.middleware.race.mom.bean.RegisterMessageWrapper;
import com.alibaba.middleware.race.mom.bean.SendResultWrapper;
import com.alibaba.middleware.race.mom.bean.SerializeWrapper;
import com.alibaba.middleware.race.mom.codec.SerializeType;
import com.alibaba.middleware.race.mom.codec.Serializer;
import com.alibaba.middleware.race.mom.store.Storage;
import com.alibaba.middleware.race.mom.util.Logger;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class Broker extends Thread {
    private DefaultEventExecutorGroup defaultEventExecutorGroup =
            new DefaultEventExecutorGroup(Parameter.SERVER_EXECUTOR_THREADS, new DefaultThreadFactory("NettyServerWorkerThread"));
    private BlockingQueue<MessageWrapper> sendQueue = new LinkedBlockingQueue<MessageWrapper>();
    private Map<String/* groupId */, Map<String/* filter */, Queue<Channel>>> consumers =
            new ConcurrentHashMap<String, Map<String, Queue<Channel>>>();
    private Storage storage = Parameter.STORAGE;

    public static void main(String[] args) {
        new Broker().start();
    }

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

    private SerializeType serializeType() {
        return Parameter.SERIALIZE_TYPE;
    }

    @ChannelHandler.Sharable
    private class Initializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            Serializer serializer = serializeType().serializer();
            pipeline.addLast(defaultEventExecutorGroup, "decoder", serializer.decoder());
            pipeline.addLast(defaultEventExecutorGroup, "encoder", serializer.encoder());
            pipeline.addLast(defaultEventExecutorGroup, "handler", new BrokerHandler());
        }
    }

    @ChannelHandler.Sharable
    class BrokerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) {
            Logger.info("[channel read] %s", msg);
            if (msg instanceof List) {
                for (Object o : (List) msg) {
                    ctx.channel().eventLoop().submit(new RequestWorker(ctx, (SerializeWrapper) o));
                }
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

    private class RequestWorker implements Runnable {
        private final SerializeWrapper wrapper;
        private final ChannelHandlerContext ctx;

        public RequestWorker(ChannelHandlerContext ctx, SerializeWrapper wrapper) {
            this.ctx = ctx;
            this.wrapper = wrapper;
        }

        @Override
        public void run() {
            Serializer serializer = serializeType().serializer();
            if (wrapper instanceof MessageWrapper) {
                Logger.info("[normal message]");
                MessageWrapper message = (MessageWrapper) wrapper;
                SendResult result;
                if (storage.insert(message.toStorage())) {
                    result = SendResult.success(message.msgId());
                    sendQueue.offer(message);
                } else {
                    result = SendResult.fail(message.msgId(), "fail to save message");
                }
                ctx.writeAndFlush(new SendResultWrapper().serialize(result, serializer));
            } else if (wrapper instanceof RegisterMessageWrapper) {
                Logger.info("[register message]");
            }
            //TODO finish handle request logic
        }
    }
}
