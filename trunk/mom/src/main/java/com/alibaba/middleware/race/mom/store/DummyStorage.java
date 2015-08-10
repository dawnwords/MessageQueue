package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.bean.MessageId;
import com.alibaba.middleware.race.mom.util.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Dawnwords on 2015/8/8.
 */
public class DummyStorage implements Storage {

    private ConcurrentHashMap<MessageId, MessageAndState> storage = new ConcurrentHashMap<MessageId, MessageAndState>();

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public boolean insert(StorageUnit unit) {
        MessageId msgId = unit.msgId();
        Logger.info("[storage insert] id:%s", msgId);
        storage.put(msgId, new MessageAndState(unit, MessageState.FAIL));
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
    public List<StorageUnit> failList() {
        List<StorageUnit> result = new LinkedList<StorageUnit>();
        for (MessageAndState state : storage.values()) {
            if (state.state == MessageState.FAIL) {
                state.state = MessageState.RESEND;
                result.add(state.unit);
            }
        }
        Logger.info("[fail list] %s", result);
        return result;
    }

    private class MessageAndState {
        private StorageUnit unit;
        private MessageState state;

        public MessageAndState(StorageUnit unit, MessageState state) {
            this.unit = unit;
            this.state = state;
        }

        @Override
        public String toString() {
            return "MessageAndState{" +
                    "state=" + state +
                    ", id=" + unit.msgId() + '}';
        }
    }
}
