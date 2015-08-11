package com.alibaba.middleware.race.mom;

import com.alibaba.middleware.race.mom.bean.*;
import com.alibaba.middleware.race.mom.codec.Decoder;
import com.alibaba.middleware.race.mom.codec.Encoder;
import com.alibaba.middleware.race.mom.store.Storage;
import com.alibaba.middleware.race.mom.store.StorageUnit;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class Broker {
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private BlockingQueue<MessageWrapper> sendQueue;
    private ConsumerPool consumers;
    private volatile boolean stop;
    private AtomicBoolean fetchFailList;
    private Storage storage;

    public Broker() {
        storage = Parameter.STORAGE;
        defaultEventExecutorGroup = new DefaultEventExecutorGroup(Parameter.SERVER_EXECUTOR_THREADS, new DefaultThreadFactory("NettyServerWorkerThread"));
        sendQueue = new LinkedBlockingQueue<MessageWrapper>();
        consumers = new ConsumerPool(storage);
        fetchFailList = new AtomicBoolean(false);
    }

    public static void main(String[] args) {
        new Broker().start();
    }

    public void start() {
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
                                stop = true;
                                workerGroup.shutdownGracefully();
                                bossGroup.shutdownGracefully();
                                defaultEventExecutorGroup.shutdownGracefully();
                                storage.stop();
                            }
                        });
                    }
                });
        storage.start();
        ExecutorService threadPool = Executors.newCachedThreadPool();
        threadPool.submit(new TimeoutWorker());
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
                consumers.removeConsumerByChannel(ctx.channel());
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
                handleMessageWrapper((MessageWrapper) wrapper);
            } else if (wrapper instanceof RegisterMessageWrapper) {
                handleRegisterMessageWrapper((RegisterMessageWrapper) wrapper);
            } else if (wrapper instanceof ConsumeResultWrapper) {
                handleConsumeResultWrapper((ConsumeResultWrapper) wrapper);
            }
        }

        private void handleConsumeResultWrapper(ConsumeResultWrapper wrapper) {
            ConsumeResult result = wrapper.deserialize();
            Logger.info("[consume result] %s", result);
            consumers.receiveConsumeResult(result);
        }

        private void handleRegisterMessageWrapper(RegisterMessageWrapper wrapper) {
            RegisterMessage register = wrapper.deserialize();
            Logger.info("[register message] %s", register);
            consumers.registerConsumer(register, ctx.channel());
        }

        private void handleMessageWrapper(MessageWrapper message) {
            Logger.info("[normal message]");
            SendResult result;
            if (storage.insert(message.toStorage())) {
                result = SendResult.success(message.msgId());
                sendQueue.offer(message);
            } else {
                result = SendResult.fail(message.msgId(), "fail to save message");
            }
            Logger.info("[send result] %s", result);
            ctx.writeAndFlush(new SendResultWrapper().serialize(result));
        }
    }

    private class MessageWorker implements Runnable {
        @Override
        public void run() {
            while (!stop) {
                boolean shouldLoad = sendQueue.size() <= 1;
                if (shouldLoad && fetchFailList.compareAndSet(false, true)) {
                    List<StorageUnit> failList = storage.failList();
                    if (failList.size() > 0) {
                        for (StorageUnit message : failList) {
                            sendQueue.add(new MessageWrapper().fromStorage(message));
                        }
                        Logger.info("[reload messages] size = %d", sendQueue.size());
                    } else {
                        try {
                            Thread.sleep(Parameter.BROKER_MESSAGE_RELOAD_FREQUENCY);
                        } catch (InterruptedException ignored) {
                        }
                    }
                    fetchFailList.set(false);
                } else {
                    try {
                        MessageWrapper message = sendQueue.take();
                        consumers.deliverMessage(message);
                    } catch (InterruptedException e) {
                        Logger.error(e);
                    }
                }
            }
        }
    }

    private class TimeoutWorker implements Runnable {
        @Override
        public void run() {
            while (!stop) {
                consumers.removeConsumeResultTimeout();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}