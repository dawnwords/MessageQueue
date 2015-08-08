package com.alibaba.middleware.race.mom;

import com.alibaba.middleware.race.mom.bean.SerializeWrapper;
import com.alibaba.middleware.race.mom.codec.SerializeType;
import com.alibaba.middleware.race.mom.codec.Serializer;
import com.alibaba.middleware.race.mom.util.Logger;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by Dawnwords on 2015/8/8.
 */
public abstract class DefaultClient {
    private final Bootstrap bootstrap;
    private final NioEventLoopGroup eventLoopGroup;
    protected Channel channel;
    private SerializeType serializeType;

    public DefaultClient() {
        this.serializeType = Parameter.SERIALIZE_TYPE;
        this.eventLoopGroup = new NioEventLoopGroup(Parameter.CLIENT_THREADS, new DefaultThreadFactory("NettyClientSelector"));
        this.bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new Initializer())
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_SNDBUF, Parameter.SND_BUF_SIZE)
                .option(ChannelOption.SO_RCVBUF, Parameter.RCV_BUF_SIZE);
    }

    protected abstract void handleMessage(ChannelHandlerContext ctx, Object msg) throws Exception;

    public void start() {
        channel = bootstrap
                .connect(System.getProperty("SIP", Parameter.SERVER_IP), Parameter.SERVER_PORT)
                .syncUninterruptibly()
                .channel();
    }

    public void stop() {
        channel.closeFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                eventLoopGroup.shutdownGracefully();
            }
        });
    }

    protected Serializer serializer() {
        return serializeType.serializer();
    }

    @ChannelHandler.Sharable
    private class Initializer extends ChannelInitializer<SocketChannel> {

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            Serializer serializer = serializeType.serializer();
            pipeline.addLast("decoder", serializer.decoder());
            pipeline.addLast("encoder", serializer.encoder());
            pipeline.addLast("handler", new MessageHandler());
        }
    }

    @ChannelHandler.Sharable
    class MessageHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            Serializer serializer = serializer();
            if (msg instanceof List) {
                for (Object o : (List) msg) {
                    Object message = ((SerializeWrapper) o).deserialize(serializer);
                    Logger.info("[handle message] %s", message);
                    handleMessage(ctx, message);
                }
            } else {
                Logger.error("[unknown response type]" + msg.getClass().getName());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            Logger.error(cause);
        }
    }
}
