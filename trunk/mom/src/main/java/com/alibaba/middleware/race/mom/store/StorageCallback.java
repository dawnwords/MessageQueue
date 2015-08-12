package com.alibaba.middleware.race.mom.store;

/**
 * Created by Dawnwords on 2015/8/11.
 */
public interface StorageCallback<T> {
    void complete(T result);
}
