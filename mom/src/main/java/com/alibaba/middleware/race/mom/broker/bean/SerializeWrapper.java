package com.alibaba.middleware.race.mom.broker.bean;


import com.alibaba.middleware.race.mom.broker.codec.Serializer;

import java.io.Serializable;

/**
 * Created by Dawnwords on 2015/7/31.
 */
public interface SerializeWrapper<T> extends Serializable {
    T deserialize(Serializer serializer);

    SerializeWrapper<T> serialize(T obj, Serializer serializer);
}
