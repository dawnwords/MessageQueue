package com.alibaba.middleware.race.mom.store;

/**
 * Created by Dawnwords on 2015/8/8.
 */
public interface Storable<T> {
    byte[] toStorage();

    T fromStorage(byte[] key, byte[] bytes);
}
