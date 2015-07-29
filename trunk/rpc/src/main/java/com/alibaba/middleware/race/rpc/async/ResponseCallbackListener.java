package com.alibaba.middleware.race.rpc.async;

/**
 * Created by huangsheng.hs on 2015/3/27.
 */
public interface ResponseCallbackListener {
    void onResponse(Object response);

    void onTimeout();

    void onException(Exception e);

    ResponseCallbackListener NULL = new ResponseCallbackListener() {
        @Override
        public void onResponse(Object response) {
        }

        @Override
        public void onTimeout() {
        }

        @Override
        public void onException(Exception e) {
        }
    };
}
