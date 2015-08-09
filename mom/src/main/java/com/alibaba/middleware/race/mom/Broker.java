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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class Broker {
    private static final String NULL_FILTER = "[]";
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private BlockingQueue<MessageWrapper> sendQueue;
    private Map<String/* groupId */, Map<String/* filter */, Queue<Channel>>> consumers;
    private Map<MessageId, BlockingQueue<ConsumeResult>> sendResultMap;
    private volatile boolean stop;
    private AtomicBoolean fetchFailList;
    private Storage storage;

    public Broker() {
        defaultEventExecutorGroup = new DefaultEventExecutorGroup(Parameter.SERVER_EXECUTOR_THREADS, new DefaultThreadFactory("NettyServerWorkerThread"));
        sendQueue = new LinkedBlockingQueue<MessageWrapper>();
        consumers = new ConcurrentHashMap<String, Map<String, Queue<Channel>>>();
        sendResultMap = new ConcurrentHashMap<MessageId, BlockingQueue<ConsumeResult>>();
        fetchFailList = new AtomicBoolean(false);
        storage = Parameter.STORAGE;
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
                if (storage.insert(message.msgId().id(), message.toStorage())) {
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
                filter = filter == null || "".equals(filter) ? NULL_FILTER : filter;
                Queue<Channel> channels = filterChannelQueue.get(filter);
                if (channels == null) {
                    channels = new ConcurrentLinkedQueue<Channel>();
                    filterChannelQueue.put(filter, channels);
                }
                channels.add(ctx.channel());
            } else if (wrapper instanceof ConsumeResultWrapper) {
                ConsumeResult result = ((ConsumeResultWrapper) wrapper).deserialize();
                Logger.info("[consume result] %s", result);
                MessageId msgId = result.msgId();
                BlockingQueue<ConsumeResult> resultHolder = sendResultMap.get(msgId);
                if (resultHolder != null) {
                    resultHolder.offer(result);
                } else {
                    Logger.error("[unknown send result id] %s", msgId);
                }
            }
        }
    }

    private class MessageWorker implements Runnable {

        @Override
        public void run() {
            while (!stop) {
                boolean shouldLoad = sendQueue.size() <= 1;
                if (shouldLoad && fetchFailList.compareAndSet(false, true)) {
                    List<byte[]> failList = storage.failList();
                    if (failList.size() > 0) {
                        for (byte[] message : failList) {
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
                        String topic = message.topic();
                        String filter = message.filter();

                        Map<String, Queue<Channel>> filterChannelMap = consumers.get(topic);
                        if (filterChannelMap != null) {
                            if (filter == null) {
                                deliverMessageToNullFiltered(message, filterChannelMap);
                            } else {
                                Queue<Channel> channels = filterChannelMap.get(filter);
                                if (channels != null) {
                                    deliverMessage(message, channels);
                                } else {
                                    deliverMessageToNullFiltered(message, filterChannelMap);
                                }
                            }
                        } else {
                            storage.markFail(message.msgId().id());
                            Logger.info("[no user for topic] %s", topic);
                        }
                    } catch (InterruptedException e) {
                        Logger.error(e);
                    }
                }
            }
        }

        private void deliverMessage(MessageWrapper message, Queue<Channel> channels) {
            Channel consumer = null;
            while (!channels.isEmpty()) {
                consumer = channels.poll();
                if (consumer.isActive()) {
                    break;
                }
            }
            if (consumer != null) {
                consumer.writeAndFlush(message);
                channels.add(consumer);
                Logger.info("[send message to] %s: %s", consumer, message);

                BlockingQueue<ConsumeResult> result = new LinkedBlockingQueue<ConsumeResult>(1);
                MessageId msgId = message.msgId();
                sendResultMap.put(msgId, result);
                ConsumeResult consumeResult = null;
                try {
                    consumeResult = result.poll(Parameter.BROKER_TIME_OUT_SECOND, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
                if (consumeResult == null) {
                    storage.markFail(msgId.id());
                    Logger.info("[receive send result timeout] %s", msgId);
                } else {
                    switch (consumeResult.getStatus()) {
                        case SUCCESS:
                            storage.markSuccess(msgId.id());
                            break;
                        case FAIL:
                            storage.markFail(msgId.id());
                            break;
                        default:
                            Logger.error("[unknown SendResult status] %s", consumeResult.getStatus());
                    }
                }
            } else {
                Logger.error("[no user for current channel]");
            }
        }

        private void deliverMessageToNullFiltered(MessageWrapper message, Map<String, Queue<Channel>> filterChannelMap) {
            Queue<Channel> channels = filterChannelMap.get(NULL_FILTER);
            if (channels != null) {
                deliverMessage(message, channels);
            } else {
                Logger.error("[no user for null filter]");
            }
        }
    }
}