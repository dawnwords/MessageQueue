package com.alibaba.middleware.race.rpc.api.netty;

import com.alibaba.middleware.race.rpc.model.RpcRequest;
import com.alibaba.middleware.race.rpc.model.RpcResponse;
import io.netty.channel.*;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Created by Dawnwords on 2015/7/25.
 */
@ChannelHandler.Sharable
public class ServerRpcHandler extends SimpleChannelInboundHandler<RpcRequest> {

    private Class<?> serviceInterface;
    private Object serviceInstance;
    private String version;

    public ServerRpcHandler(Class<?> serviceInterface, Object serviceInstance, String version) {
        this.serviceInterface = serviceInterface;
        this.serviceInstance = serviceInstance;
        this.version = version;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcRequest request) throws Exception {
        request.restoreContext();
        System.out.println("[receive request]" + request);
        RpcResponse response = new RpcResponse();

        if (version != null && !version.equals(request.version())) {
            response.exception(new IllegalStateException(String.format("version not match: provided: %s, given :%s", version, request.version())));
        } else {
            try {
                Method method = serviceInterface.getDeclaredMethod(request.methodName(), request.parameterTypes());
                response.appResponse((Serializable) method.invoke(serviceInstance, request.arguments()));
            } catch (InvocationTargetException e) {
                response.exception(e.getTargetException());
            } catch (Exception e) {
                response.exception(e);
            }
        }
        writeResponse(ctx, response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        writeResponse(ctx, new RpcResponse().exception(cause));
    }

    private void writeResponse(ChannelHandlerContext ctx, RpcResponse response) {
        System.out.println("[send response]" + response);
        ctx.writeAndFlush(response);
        ctx.close();
    }
}
