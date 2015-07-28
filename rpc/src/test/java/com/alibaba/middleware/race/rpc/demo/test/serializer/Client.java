package com.alibaba.middleware.race.rpc.demo.test.serializer;

import com.alibaba.middleware.race.rpc.api.Parameter;
import com.alibaba.middleware.race.rpc.api.RpcConsumer;
import com.alibaba.middleware.race.rpc.api.codec.SerializeType;
import com.alibaba.middleware.race.rpc.api.util.Logger;
import com.alibaba.middleware.race.rpc.demo.service.RaceDO;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Dawnwords on 2015/7/21.
 */
public class Client extends RpcConsumer {
    private SerializeType serializeType;
    private Bootstrap connector;
    private CountDownLatch countDownLatch;
    private AtomicInteger callAmount;

    public Client(SerializeType serializeType) {
        this.serializeType = serializeType;
        this.connector = new Bootstrap()
                .group(new NioEventLoopGroup(1, new DefaultThreadFactory("NettyClientSelector")))
                .channel(NioSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.SO_SNDBUF, Parameter.SND_BUF_SIZE)
                .option(ChannelOption.SO_RCVBUF, Parameter.RCV_BUF_SIZE);
    }

    public double getTPS() {
        int coreCount = Runtime.getRuntime().availableProcessors() * 2;
        countDownLatch = new CountDownLatch(coreCount);
        callAmount = new AtomicInteger(0);

        long timeMillis = System.currentTimeMillis();

        final ExecutorService executor = Executors.newFixedThreadPool(coreCount);
        for (int i = 0; i < coreCount; i++) {
            executor.execute(new Runnable() {
                public void run() {
                    while (callAmount.get() < 100000) {
                        send(new RaceDO());
                    }
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }

        timeMillis = System.currentTimeMillis() - timeMillis;

        return callAmount.get() / (timeMillis / 1000.0);
    }

    private void send(final RaceDO raceDO) {
        final CountDownLatch latch = new CountDownLatch(1);
        connector.handler(new Initializer())
                .connect(System.getProperty("SIP", Parameter.SERVER_IP), Parameter.SERVER_PORT)
                .addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        final Channel channel = future.channel();
                        Logger.info("send:%s", raceDO);
                        channel.eventLoop().submit(new Runnable() {
                            @Override
                            public void run() {
                                channel.writeAndFlush(raceDO).addListener(new ChannelFutureListener() {
                                    @Override
                                    public void operationComplete(ChannelFuture future) throws Exception {
                                        future.channel().closeFuture();
                                        latch.countDown();
                                    }
                                });
                            }
                        });
                    }
                });
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
    }

    @ChannelHandler.Sharable
    private class Initializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("decoder", serializeType.deserializer());
            pipeline.addLast("encoder", serializeType.serializer());
            pipeline.addLast("handler", new ClientRpcHandler());
        }
    }

    @ChannelHandler.Sharable
    private class ClientRpcHandler extends SimpleChannelInboundHandler<RaceDO> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaceDO raceDO) throws Exception {
            Logger.info("receive:%s", raceDO);
            if (new RaceDO().equals(raceDO)) {
                callAmount.incrementAndGet();
            }
        }
    }

    public static void main(String[] args) {
        System.out.printf("%s tps: %f\n", SerializeType.kryo, new Client(SerializeType.kryo).getTPS());
        System.exit(0);
    }
}
