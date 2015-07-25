package com.alibaba.middleware.race.rpc.api.impl;

import com.alibaba.middleware.race.rpc.api.Parameter;
import com.alibaba.middleware.race.rpc.api.RpcProvider;
import com.alibaba.middleware.race.rpc.api.serializer.SerializeType;
import com.alibaba.middleware.race.rpc.model.RpcRequest;
import com.alibaba.middleware.race.rpc.model.RpcResponse;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Dawnwords on 2015/7/21.
 */
public class RpcProviderImpl extends RpcProvider {

    private Class<?> serviceInterface;
    private String version;
    private int timeout;
    private SerializeType serializeType;
    private Object serviceInstance;

    @Override
    public RpcProvider serviceInterface(Class<?> serviceInterface) {
        this.serviceInterface = serviceInterface;
        return this;
    }

    @Override
    public RpcProvider version(String version) {
        this.version = version;
        return this;
    }

    @Override
    public RpcProvider impl(Object serviceInstance) {
        this.serviceInstance = serviceInstance;
        return this;
    }

    @Override
    public RpcProvider timeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public RpcProvider serializeType(String serializeType) {
        this.serializeType = SerializeType.valueOf(serializeType);
        return this;
    }

    @Override
    public void publish() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(Parameter.SERVER_PORT);
            System.out.println("[start provider] at " + Parameter.SERVER_PORT);
            while (true) {
                Socket client = socket.accept();
                client.setSoTimeout(timeout);
                System.out.println("[client connected]:" + client.getInetAddress().getHostAddress());
                new ClientHandler(client).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close(socket);
        }
    }

    private static void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignored) {
            }
        }
    }

    private class ClientHandler extends Thread {
        private Socket client;

        public ClientHandler(Socket client) {
            this.client = client;
        }

        @Override
        public void run() {
            RpcResponse response = new RpcResponse();
            try {
                RpcRequest request = (RpcRequest) serializeType.deserialize(client.getInputStream());
                request.restoreContext();
                System.out.printf("[receive request] from %s: %s\n",
                        client.getInetAddress().getHostAddress(), request.toString());
                if (version != null && !version.equals(request.version())) {
                    throw new RuntimeException(
                            String.format("version not match: provided: %s, given :%s", version, request.version()));
                }
                Method method = serviceInterface.getDeclaredMethod(request.methodName(), request.parameterTypes());
                response.appResponse((Serializable) method.invoke(serviceInstance, request.arguments()));
            } catch (InvocationTargetException e) {
                response.exception(e.getTargetException());
            } catch (Exception e) {
                response.exception(e);
            }

            System.out.println("[send response]:" + response.toString());
            try {
                serializeType.serialize(response, client.getOutputStream());
            } catch (Exception ignored) {
            } finally {
                close(client);
            }
        }
    }
}
