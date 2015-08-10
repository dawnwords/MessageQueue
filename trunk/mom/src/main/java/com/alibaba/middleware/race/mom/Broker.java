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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Dawnwords on 2015/8/6.
 */
public class Broker {
    private static final String NULL_FILTER = "[]";
    private DefaultEventExecutorGroup defaultEventExecutorGroup;
    private BlockingQueue<MessageWrapper> sendQueue;
    private Map<String/* groupId */, Map<String/* filter */, Queue<ConsumerHolder>>> consumers;
    private Map<ConsumerHolder, TopicFilter> consumerIndex;
    private Map<MessageId, Long/* create time */> consumeResult;
    private volatile boolean stop;
    private AtomicBoolean fetchFailList;
    private Storage storage;

    public Broker() {
        defaultEventExecutorGroup = new DefaultEventExecutorGroup(Parameter.SERVER_EXECUTOR_THREADS, new DefaultThreadFactory("NettyServerWorkerThread"));
        sendQueue = new LinkedBlockingQueue<MessageWrapper>();
        consumers = new ConcurrentHashMap<String, Map<String, Queue<ConsumerHolder>>>();
        consumerIndex = new ConcurrentHashMap<ConsumerHolder, TopicFilter>();
        consumeResult = new ConcurrentHashMap<MessageId, Long>();
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
                ConsumerHolder key = new ConsumerHolder(ctx.channel());
                TopicFilter topicFilter = consumerIndex.get(key);
                if (topicFilter != null) {
                    Map<String, Queue<ConsumerHolder>> filterConsumerMap = consumers.get(topicFilter.topic);
                    if (filterConsumerMap != null) {
                        Queue<ConsumerHolder> consumers = filterConsumerMap.get(topicFilter.filter);
                        if (consumers != null) {
                            consumers.remove(key);
                            Logger.info("[consumer disconnected] %s", key);
                            ctx.close();
                            return;
                        }
                    }
                    Logger.error("[no such consumer] %s", key);
                } else {
                    Logger.info("[producer disconnected] %s", key);
                }
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
                String topic = register.topic();
                Map<String, Queue<ConsumerHolder>> filterChannelQueue = consumers.get(topic);
                if (filterChannelQueue == null) {
                    filterChannelQueue = new ConcurrentHashMap<String, Queue<ConsumerHolder>>();
                    consumers.put(topic, filterChannelQueue);
                }
                String filter = register.filter();
                filter = filter == null || "".equals(filter) ? NULL_FILTER : filter;
                Queue<ConsumerHolder> channels = filterChannelQueue.get(filter);
                if (channels == null) {
                    channels = new ConcurrentLinkedQueue<ConsumerHolder>();
                    filterChannelQueue.put(filter, channels);
                }
                ConsumerHolder consumer = new ConsumerHolder(ctx.channel());
                channels.add(consumer);
                consumerIndex.put(consumer, new TopicFilter(topic, filter));
            } else if (wrapper instanceof ConsumeResultWrapper) {
                ConsumeResult result = ((ConsumeResultWrapper) wrapper).deserialize();
                Logger.info("[consume result] %s", result);
                MessageId msgId = result.msgId();
                if (consumeResult.remove(msgId) != null) {
                    switch (result.getStatus()) {
                        case SUCCESS:
                            storage.markSuccess(msgId.id());
                            break;
                        case FAIL:
                            storage.markFail(msgId.id());
                            break;
                        default:
                            Logger.error("[unknown ConsumeResult status] %s", result.getStatus());
                    }
                } else {
                    Logger.error("[unknown ConsumeResult id] %s", msgId);
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

                        Map<String, Queue<ConsumerHolder>> filterChannelMap = consumers.get(topic);
                        if (filterChannelMap != null) {
                            if (filter == null) {
                                deliverMessageToNullFiltered(message, filterChannelMap);
                            } else {
                                Queue<ConsumerHolder> channels = filterChannelMap.get(filter);
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

        private void deliverMessage(MessageWrapper message, Queue<ConsumerHolder> channels) {
            ConsumerHolder consumer = channels.poll();
            if (consumer != null) {
                consumer.channel.writeAndFlush(message);
                channels.add(consumer);
                Logger.info("[send message to] %s: %s", consumer, message);
                consumeResult.put(message.msgId(), System.currentTimeMillis());
            } else {
                Logger.error("[no user for current channel]");
            }
        }

        private void deliverMessageToNullFiltered(MessageWrapper message, Map<String, Queue<ConsumerHolder>> filterChannelMap) {
            Queue<ConsumerHolder> channels = filterChannelMap.get(NULL_FILTER);
            if (channels != null) {
                deliverMessage(message, channels);
            } else {
                Logger.error("[no user for null filter]");
            }
        }
    }

    private class TimeoutWorker implements Runnable {

        @Override
        public void run() {
            while (!stop) {
                long current = System.currentTimeMillis();
                for (MessageId id : consumeResult.keySet()) {
                    if (current - consumeResult.get(id) > Parameter.BROKER_TIME_OUT) {
                        consumeResult.remove(id);
                        storage.markFail(id.id());
                        Logger.info("[consume result timeout] %s", id);
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private class ConsumerHolder {
        private InetSocketAddress remote, local;
        private Channel channel;

        ConsumerHolder(Channel channel) {
            this.remote = (InetSocketAddress) channel.remoteAddress();
            this.local = (InetSocketAddress) channel.localAddress();
            this.channel = channel;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConsumerHolder that = (ConsumerHolder) o;
            return remote.equals(that.remote) && local.equals(that.local);
        }

        @Override
        public int hashCode() {
            int result = remote.hashCode();
            result = 31 * result + local.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "ConsumerHolder{" +
                    "remote=" + remote +
                    ", local=" + local +
                    '}';
        }
    }

    private class TopicFilter {
        private String topic, filter;

        TopicFilter(String topic, String filter) {
            this.topic = topic;
            this.filter = filter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicFilter that = (TopicFilter) o;
            return topic.equals(that.topic) && !(filter != null ? !filter.equals(that.filter) : that.filter != null);

        }

        @Override
        public int hashCode() {
            int result = topic.hashCode();
            result = 31 * result + (filter != null ? filter.hashCode() : 0);
            return result;
        }
    }
}