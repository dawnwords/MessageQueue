package com.alibaba.middleware.race.rpc.aop;

import com.alibaba.middleware.race.rpc.model.RpcRequest;

/**
 * Created by huangsheng.hs on 2015/5/7.
 */
public interface ProviderHook {
    void before(RpcRequest request);
    void after(RpcRequest request);
}
