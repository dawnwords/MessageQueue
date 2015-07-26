package com.alibaba.middleware.race.rpc.api.netty;

import com.alibaba.middleware.race.rpc.api.util.Logger;
import com.alibaba.middleware.race.rpc.async.ResponseCallbackListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.util.concurrent.TimeUnit;

/**
 * Created by Dawnwords on 2015/7/26.
 */
@ChannelHandler.Sharable
public class ClientTimeoutHandler extends ReadTimeoutHandler {
    private ResponseCallbackListener listener;

    public ClientTimeoutHandler(int timeout, ResponseCallbackListener listener) {
        super(timeout, TimeUnit.MILLISECONDS);
        this.listener = listener;
    }

    @Override
    protected void readTimedOut(ChannelHandlerContext ctx) throws Exception {
        super.readTimedOut(ctx);
        if (listener != null) {
            listener.onTimeout();
        }
        Logger.info("[client read time out]");
        ctx.close();
    }
}
