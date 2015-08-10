package com.alibaba.middleware.race.mom.store;

import java.nio.ByteBuffer;

/**
 * Created by Dawnwords on 2015/8/8.
 */
public interface Storable<T> {

    StorageUnit toStorage();

    T fromStorage(StorageUnit unit);
}
