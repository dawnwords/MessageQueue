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

/**
 * Created by Dawnwords on 2015/7/21.
 */
public class Client extends RpcConsumer {
    private SerializeType serializeType;
    private Channel channel;
    private CountDownLatch countDownLatch;
    private AtomicInteger callAmount;

    public Client(SerializeType serializeType) {
        this.serializeType = serializeType;
        this.channel = new Bootstrap()
                .group(new NioEventLoopGroup(1, new DefaultThreadFactory("NettyClientSelector")))
                .channel(NioSocketChannel.class)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_SNDBUF, Parameter.SND_BUF_SIZE)
                .option(ChannelOption.SO_RCVBUF, Parameter.RCV_BUF_SIZE)
                .handler(new Initializer())
                .connect(System.getProperty("SIP", Parameter.SERVER_IP), Parameter.SERVER_PORT)
                .syncUninterruptibly()
                .channel();
        Logger.error("[client start]");
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
                        RaceDO raceDO = new RaceDO();
                        Logger.info("[send] %s", raceDO);
                        channel.writeAndFlush(raceDO).syncUninterruptibly();
                    }
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await(300, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }

        timeMillis = System.currentTimeMillis() - timeMillis;
        Logger.error("[time cost]%.3fs", timeMillis / 1000.0);
        return callAmount.get() / (timeMillis / 1000.0);
    }

    @ChannelHandler.Sharable
    private class Initializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("decoder", serializeType.serializer().decoder());
            pipeline.addLast("encoder", serializeType.serializer().encoder());
            pipeline.addLast("handler", new ClientRpcHandler());
        }
    }

    @ChannelHandler.Sharable
    private class ClientRpcHandler extends SimpleChannelInboundHandler<RaceDO> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaceDO raceDO) throws Exception {
            Logger.info("[receive] %s", raceDO);
            if (new RaceDO().equals(raceDO)) {
                callAmount.incrementAndGet();
            }
        }
    }

    public static void main(String[] args) {
        System.out.printf("%s tps: %f\n", TestSerializeType.serializeType(),
                new Client(TestSerializeType.serializeType()).getTPS());
        System.exit(0);
    }
}
