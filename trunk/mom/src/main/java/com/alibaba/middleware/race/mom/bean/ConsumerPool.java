package com.alibaba.middleware.race.mom.bean;

import com.alibaba.middleware.race.mom.ConsumeResult;
import com.alibaba.middleware.race.mom.Parameter;
import com.alibaba.middleware.race.mom.store.Storage;
import com.alibaba.middleware.race.mom.util.Logger;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Dawnwords on 2015/8/11.
 */
public class ConsumerPool {
    private static final String NULL_FILTER = "[]";

    private Map<String/* groupId */, Map<String/* filter */, BlockingQueue<ConsumerHolder>>> consumers;
    private Map<ConsumerHolder, TopicFilter> consumerIndex;
    private Map<MessageId, Long/* create time */> consumeResult;
    private Storage storage;

    public ConsumerPool(Storage storage) {
        this.storage = storage;
        this.consumers = new ConcurrentHashMap<String, Map<String, BlockingQueue<ConsumerHolder>>>();
        this.consumerIndex = new ConcurrentHashMap<ConsumerHolder, TopicFilter>();
        this.consumeResult = new ConcurrentHashMap<MessageId, Long>();
    }

    public void removeConsumerByChannel(Channel channel) {
        ConsumerHolder key = new ConsumerHolder(channel);
        TopicFilter topicFilter = consumerIndex.get(key);
        if (topicFilter != null) {
            Map<String, BlockingQueue<ConsumerHolder>> filterConsumerMap = consumers.get(topicFilter.topic);
            if (filterConsumerMap != null) {
                Queue<ConsumerHolder> consumers = filterConsumerMap.get(topicFilter.filter);
                if (consumers != null) {
                    consumers.remove(key);
                    Logger.info("[consumer disconnected] %s", key);
                    return;
                }
            }
            Logger.error("[no such consumer] %s", key);
        } else {
            Logger.info("[producer disconnected] %s", key);
        }
    }

    public void registerConsumer(RegisterMessage register, Channel channel) {
        String topic = register.topic();
        Map<String, BlockingQueue<ConsumerHolder>> filterChannelQueue = consumers.get(topic);
        if (filterChannelQueue == null) {
            filterChannelQueue = new ConcurrentHashMap<String, BlockingQueue<ConsumerHolder>>();
            consumers.put(topic, filterChannelQueue);
        }
        String filter = register.filter();
        filter = filter == null || "".equals(filter) ? NULL_FILTER : filter;
        BlockingQueue<ConsumerHolder> channels = filterChannelQueue.get(filter);
        if (channels == null) {
            channels = new LinkedBlockingQueue<ConsumerHolder>();
            filterChannelQueue.put(filter, channels);
        }
        ConsumerHolder consumer = new ConsumerHolder(channel);
        channels.add(consumer);
        consumerIndex.put(consumer, new TopicFilter(topic, filter));
    }

    public void deliverMessage(MessageWrapper message) {
        String topic = message.topic();
        String filter = message.filter();

        Map<String, BlockingQueue<ConsumerHolder>> filterChannelMap = consumers.get(topic);
        if (filterChannelMap != null) {
            if (filter == null) {
                deliverMessageToNullFiltered(message, filterChannelMap);
            } else {
                BlockingQueue<ConsumerHolder> channels = filterChannelMap.get(filter);
                if (channels != null) {
                    deliverMessage(message, channels);
                } else {
                    deliverMessageToNullFiltered(message, filterChannelMap);
                }
            }
        } else {
            storage.markFail(message.msgId());
            Logger.info("[no user for topic] %s", topic);
        }
    }

    private void deliverMessage(MessageWrapper message, BlockingQueue<ConsumerHolder> channels) {
        try {
            ConsumerHolder consumer = channels.take();
            consumeResult.put(message.msgId(), System.currentTimeMillis());
            consumer.channel.writeAndFlush(message);
            channels.add(consumer);
            Logger.info("[send message to] %s: %s", consumer, message);
        } catch (InterruptedException ignored) {
            Logger.error("[get free consumer interrupted]");
        }
    }

    private void deliverMessageToNullFiltered(MessageWrapper message, Map<String, BlockingQueue<ConsumerHolder>> filterChannelMap) {
        BlockingQueue<ConsumerHolder> channels = filterChannelMap.get(NULL_FILTER);
        if (channels != null) {
            deliverMessage(message, channels);
        } else {
            Logger.error("[no user for null filter]");
        }
    }

    public void receiveConsumeResult(ConsumeResult result) {
        MessageId msgId = result.msgId();
        if (consumeResult.remove(msgId) != null) {
            switch (result.getStatus()) {
                case SUCCESS:
                    storage.markSuccess(msgId);
                    break;
                case FAIL:
                    storage.markFail(msgId);
                    break;
                default:
                    Logger.error("[unknown ConsumeResult status] %s", result.getStatus());
            }
        } else {
            Logger.error("[unknown ConsumeResult id] %s", msgId);
        }
    }

    public void removeConsumeResultTimeout() {
        long current = System.currentTimeMillis();
        for (MessageId id : consumeResult.keySet()) {
            if (current - consumeResult.get(id) > Parameter.BROKER_TIME_OUT) {
                consumeResult.remove(id);
                storage.markFail(id);
                Logger.error("[ConsumeResult timeout] %s", id);
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
