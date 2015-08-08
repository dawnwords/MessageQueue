package com.alibaba.middleware.race.mom.store;

import java.util.Collections;
import java.util.List;

/**
 * Created by Dawnwords on 2015/8/8.
 */
public class DummyStorage implements Storage {
    @Override
    public boolean insert(byte[] content) {
        return true;
    }

    @Override
    public boolean markSuccess(byte[] id) {
        return true;
    }

    @Override
    public boolean markFail(byte[] id) {
        return true;
    }

    @Override
    public List<byte[]> failList() {
        return Collections.emptyList();
    }
}
