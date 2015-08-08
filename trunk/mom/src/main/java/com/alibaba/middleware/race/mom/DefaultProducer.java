package com.alibaba.middleware.race.mom;


import com.alibaba.middleware.race.mom.bean.MessageWrapper;
import com.alibaba.middleware.race.mom.util.Logger;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class DefaultProducer extends DefaultClient implements Producer {
    private String topic;
    private String groupId;
    private ConcurrentHashMap<Integer/* messageId */, BlockingQueue<SendResult>> sendResultMap
            = new ConcurrentHashMap<Integer, BlockingQueue<SendResult>>();

    @Override
    protected void handleMessage(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof SendResult) {
            SendResult result = (SendResult) msg;
            int msgIdHash = Arrays.hashCode(result.getMsgIdAsArray());
            BlockingQueue<SendResult> resultHolder = sendResultMap.get(msgIdHash);
            if (resultHolder != null) {
                resultHolder.put(result);
            } else {
                Logger.error("unknown message id: %s", result.getMsgId());
            }
        }
    }

    @Override
    public void start() {
        if (topic == null) {
            throw new IllegalStateException("topic not set");
        }
        if (groupId == null) {
            throw new IllegalStateException("groupId not set");
        }
        super.start();
    }

    @Override
    public void setTopic(String topic) {
        if (this.topic != null) {
            throw new IllegalStateException("topic has been set");
        }
        this.topic = topic;
    }

    @Override
    public void setGroupId(String groupId) {
        if (this.groupId != null) {
            throw new IllegalStateException("groupId has been set");
        }
        this.groupId = groupId;
    }

    @Override
    public SendResult sendMessage(Message message) {
        message.setTopic(topic);
        message.setMsgId((InetSocketAddress) channel.localAddress());
        final byte[] msgId = message.getMsgIdAsByte();
        final int msgIdHash = Arrays.hashCode(msgId);
        final ArrayBlockingQueue<SendResult> resultHolder = new ArrayBlockingQueue<SendResult>(1);
        sendResultMap.put(msgIdHash, resultHolder);
        channel.writeAndFlush(new MessageWrapper().serialize(message, serializer()))
                .addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            resultHolder.put(SendResult.fail(msgId, future.cause().toString()));
                        }
                    }
                });
        SendResult result = null;
        try {
            result = sendResultMap.get(msgIdHash).poll(Parameter.PRODUCER_TIME_OUT_SECOND, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }

        return result == null ? SendResult.fail(msgId, "SendResult Time Out") : result;
    }

    @Override
    public void asyncSendMessage(final Message message, final SendCallback callback) {
        channel.eventLoop().submit(new Runnable() {
            @Override
            public void run() {
                callback.onResult(sendMessage(message));
            }
        });
    }
}
