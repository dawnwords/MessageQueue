package com.alibaba.middleware.race.rpc.demo.service;

import java.util.Map;

/**
 * Created by huangsheng.hs on 2015/3/26.
 */
public interface RaceTestService {
    Map<String,Object> getMap();
    String getString();
    RaceDO getDO();
    boolean longTimeMethod();
    Integer throwException() throws RaceException;
}
