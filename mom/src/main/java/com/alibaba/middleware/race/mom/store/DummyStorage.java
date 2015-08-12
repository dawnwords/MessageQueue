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
    public void insert(StorageUnit unit, StorageCallback<Boolean> callback) {
        MessageId msgId = unit.msgId();
        Logger.info("[storage insert] id:%s", msgId);
        storage.put(msgId, new MessageAndState(unit, MessageState.RESEND));
        callback.complete(true);
    }

    @Override
    public void markSuccess(MessageId msgId) {
        Logger.info("[mark success] id %s", msgId);
        storage.remove(msgId);
    }

    @Override
    public void markFail(MessageId msgId) {
        Logger.info("[mark fail] id %s", msgId);
        MessageAndState state = storage.get(msgId);
        if (state != null) {
            state.state = MessageState.FAIL;
        }
    }

    @Override
    public void failList(StorageCallback<List<StorageUnit>> callback) {
        List<StorageUnit> result = new LinkedList<StorageUnit>();
        for (MessageAndState state : storage.values()) {
            if (state.state == MessageState.FAIL) {
                state.state = MessageState.RESEND;
                result.add(state.unit);
            }
        }
        Logger.info("[fail list] %s", result);
        callback.complete(result);
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
