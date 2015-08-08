package com.alibaba.middleware.race.mom;


import com.alibaba.middleware.race.mom.bean.RegisterMessage;
import com.alibaba.middleware.race.mom.bean.RegisterMessageWrapper;
import com.alibaba.middleware.race.mom.util.Logger;
import io.netty.channel.ChannelHandlerContext;

public class DefaultConsumer extends DefaultClient implements Consumer {
    private String groupId, topic, filter;
    private MessageListener listener;

    @Override
    protected void handleMessage(ChannelHandlerContext ctx, Object msg) {

    }

    @Override
    public void start() {
        if (groupId == null) {
            throw new IllegalStateException("groupId not set");
        }
        if (topic == null) {
            throw new IllegalStateException("topic not set");
        }
        if (listener == null) {
            throw new IllegalStateException("listener not set");
        }
        super.start();
        channel.writeAndFlush(new RegisterMessageWrapper().serialize(new RegisterMessage()
                .type(RegisterMessage.ClientType.Consumer)
                .groupId(groupId)
                .topic(topic)
                .filter(filter), serializer()));
    }

    @Override
    public void subscribe(String topic, String filter, MessageListener listener) {
        if (this.topic != null || this.filter != null || this.listener != null) {
            throw new IllegalStateException("has subscribed");
        }
        this.topic = topic;
        this.filter = filter;
        this.listener = listener;
    }

    @Override
    public void setGroupId(String groupId) {
        if (this.groupId != null) {
            throw new IllegalArgumentException("groupId has been set");
        }
        this.groupId = groupId;
    }


}
