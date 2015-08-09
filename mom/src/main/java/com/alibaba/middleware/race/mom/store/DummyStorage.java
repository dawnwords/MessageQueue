package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.bean.MessageId;

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
        ByteBuffer b = ByteBuffer.wrap(content);
        byte[] msgId = new byte[16];
        b.get(msgId);
        storage.put(new MessageId(msgId), new MessageAndState(content, MessageState.FAIL));
        return true;
    }

    @Override
    public boolean markSuccess(byte[] id) {
        storage.remove(new MessageId(id));
        return true;
    }

    @Override
    public boolean markFail(byte[] id) {
        MessageAndState state = storage.get(new MessageId(id));
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
        return result;
    }

    private class MessageAndState {
        private byte[] content;
        private MessageState state;

        public MessageAndState(byte[] content, MessageState state) {
            this.content = content;
            this.state = state;
        }
    }
}
