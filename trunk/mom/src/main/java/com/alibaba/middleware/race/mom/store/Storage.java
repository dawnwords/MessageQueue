package com.alibaba.middleware.race.mom.store;

import java.util.List;

/**
 * Created by Dawnwords on 2015/8/7.
 */
public interface Storage {

    boolean insert(byte[] content);

    boolean markSuccess(byte[] id);

    List<byte[]/* content */> unsuccessfulList();

}
