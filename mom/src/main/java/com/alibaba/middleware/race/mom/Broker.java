package com.alibaba.middleware.race.mom;

import com.alibaba.middleware.race.mom.bean.*;
import com.alibaba.middleware.race.mom.codec.Decoder;
import com.alibaba.middleware.race.mom.codec.Encoder;
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
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class Broker extends Thread {
    private static final String NULL_FILTER = "[]";
    private DefaultEventExecutorGroup defaultEventExecutorGroup =
            new DefaultEventExecutorGroup(Parameter.SERVER_EXECUTOR_THREADS, new DefaultThreadFactory("NettyServerWorkerThread"));
    private Queue<MessageWrapper> sendQueue = new LinkedBlockingQueue<MessageWrapper>();
    private Map<String/* groupId */, Map<String/* filter */, Queue<Channel>>> consumers =
            new ConcurrentHashMap<String, Map<String, Queue<Channel>>>();
    private Storage storage = Parameter.STORAGE;

    public static void main(String[] args) {
        new Broker().start();
    }

    @Override
    public void run() {
        final EventLoopGroup bossGroup = new NioEventLoopGroup(
                Parameter.SERVER_BOSS_THREADS, new DefaultThreadFactory("NettyBossSelector"));
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
        ExecutorService threadPool = Executors.newCachedThreadPool();
        for (int i = 0; i < Parameter.SERVER_EXECUTOR_THREADS; i++) {
            threadPool.submit(new MessageWorker());
        }
    }

    @ChannelHandler.Sharable
    private class Initializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(defaultEventExecutorGroup, "decoder", new Decoder());
            pipeline.addLast(defaultEventExecutorGroup, "encoder", new Encoder());
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
                Logger.info("[send result] %s", result);
                ctx.writeAndFlush(new SendResultWrapper().serialize(result));
            } else if (wrapper instanceof RegisterMessageWrapper) {
                RegisterMessage register = ((RegisterMessageWrapper) wrapper).deserialize();
                Logger.info("[register message] %s", register);
                Map<String, Queue<Channel>> filterChannelQueue = consumers.get(register.topic());
                if (filterChannelQueue == null) {
                    filterChannelQueue = new ConcurrentHashMap<String, Queue<Channel>>();
                    consumers.put(register.topic(), filterChannelQueue);
                }
                String filter = register.filter();
                filter = filter == null ? NULL_FILTER : filter;
                Queue<Channel> channels = filterChannelQueue.get(filter);
                if (channels == null) {
                    channels = new ConcurrentLinkedQueue<Channel>();
                    filterChannelQueue.put(filter, channels);
                }
                channels.add(ctx.channel());
            } else if (wrapper instanceof ConsumeResultWrapper) {
                ConsumeResult result = ((ConsumeResultWrapper) wrapper).deserialize();
                Logger.info("[consume result] %s", result);
                if (result.getStatus() == ConsumeStatus.SUCCESS) {
                    storage.markSuccess(result.msgId());
                }
            }
        }
    }

    private class MessageWorker implements Runnable {
        @Override
        public void run() {
            while (true) {
                boolean shouldLoad = sendQueue.size() <= 1;
                if (shouldLoad) {
                    for (byte[] message : storage.failList()) {
                        sendQueue.add(new MessageWrapper().fromStorage(message));
                    }
                } else {
                    MessageWrapper message = sendQueue.poll();
                    String topic = message.topic();
                    String filter = message.filter();

                    Map<String, Queue<Channel>> filterChannelMap = consumers.get(topic);
                    if (filterChannelMap != null) {
                        Queue<Channel> channels = filterChannelMap.get(filter);
                        if (channels != null) {
                            Channel consumer = channels.poll();
                            consumer.writeAndFlush(message);
                            channels.add(consumer);
                        } else {
                            Logger.error("[unknown filter] %s", filter);
                        }
                    } else {
                        Logger.error("[unknown topic] %s", topic);
                    }
                }
            }
        }
    }
}
