package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.bean.MessageId;
import com.alibaba.middleware.race.mom.util.Logger;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Dawnwords on 2015/8/8.
 */
public class DummyStorage implements Storage {

    private ConcurrentHashMap<MessageId, MessageAndState> storage = new ConcurrentHashMap<MessageId, MessageAndState>();

    @Override
    public boolean insert(byte[] content) {
        MessageId msgId = msgId(content);
        Logger.info("[storage insert] id:%s", msgId);
        storage.put(msgId, new MessageAndState(content, MessageState.FAIL));
        return true;
    }

    @Override
    public boolean markSuccess(byte[] id) {
        MessageId msgId = new MessageId(id);
        Logger.info("[mark success] id %s", msgId);
        storage.remove(msgId);
        return true;
    }

    @Override
    public boolean markFail(byte[] id) {
        MessageId msgId = new MessageId(id);
        Logger.info("[mark fail] id %s", msgId);
        MessageAndState state = storage.get(msgId);
        if (state == null) return false;
        state.state = MessageState.FAIL;
        return true;
    }

    @Override
    public List<byte[]> failList() {
        List<byte[]> result = new LinkedList<byte[]>();
        for (MessageAndState state : storage.values()) {
            if (state.state == MessageState.FAIL) {
                state.state = MessageState.RESEND;
                result.add(state.content);
            }
        }
        Logger.info("[fail list] %s", result);
        return result;
    }

    private MessageId msgId(byte[] content) {
        ByteBuffer b = ByteBuffer.wrap(content);
        byte[] msgId = new byte[16];
        b.get(msgId);
        return new MessageId(msgId);
    }

    private class MessageAndState {
        private byte[] content;
        private MessageState state;

        public MessageAndState(byte[] content, MessageState state) {
            this.content = content;
            this.state = state;
        }

        @Override
        public String toString() {
            return "MessageAndState{" +
                    "state=" + state +
                    ", id=" + msgId(content) + '}';
        }
    }
}
