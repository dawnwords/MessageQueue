package com.alibaba.middleware.race.rpc.model;

import com.alibaba.middleware.race.rpc.api.codec.Serializer;

/**
 * Created by Dawnwords on 2015/7/31.
 */
public class RpcResponseWrapper implements SerializeWrapper<RpcResponse> {
    private long id;
    private byte[] exception;
    private byte[] appResponse;

    @Override
    public RpcResponse deserialize(Serializer serializer) {
        return new RpcResponse()
                .id(id)
                .exception((Throwable) serializer.decode(exception))
                .appResponse(serializer.decode(appResponse));
    }

    @Override
    public RpcResponseWrapper serialize(RpcResponse response, Serializer serializer) {
        this.id = response.id();
        this.exception = serializer.encode(response.exception());
        this.appResponse = serializer.encode(response.appResponse());
        return this;
    }

    @Override
    public String toString() {
        return "RpcResponseWrapper{" +
                "id=" + id +
                ", exception=" + (exception == null ? null : exception.length) +
                ", appResponse=" + (appResponse == null ? null : appResponse.length) +
                '}';
    }
}
