package com.alibaba.middleware.race.rpc.api.netty;

import com.alibaba.middleware.race.rpc.aop.ConsumerHook;
import com.alibaba.middleware.race.rpc.async.ResponseCallbackListener;
import com.alibaba.middleware.race.rpc.model.RpcRequest;
import com.alibaba.middleware.race.rpc.model.RpcResponse;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Created by Dawnwords on 2015/7/26.
 */
@ChannelHandler.Sharable
public class ClientRpcHandler extends SimpleChannelInboundHandler<RpcResponse> {
    private final ResponseCallbackListener listener;
    private final ConsumerHook hook;
    private final RpcRequest request;

    public ClientRpcHandler(ResponseCallbackListener listener, ConsumerHook hook, RpcRequest request) {
        this.listener = listener;
        this.hook = hook;
        this.request = request;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse response) throws Exception {
        System.out.println("[receive response]" + response);
        if (listener != null) {
            if (response.hasException()) {
                listener.onException(response.exception());
            } else {
                listener.onResponse(response.appResponse());
            }
        }
        after(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (listener != null && cause instanceof Exception) {
            listener.onException((Exception) cause);
        }
        after(ctx);
    }

    private void after(ChannelHandlerContext ctx) {
        if (hook != null) {
            hook.after(request);
        }
        ctx.close();
    }

}
