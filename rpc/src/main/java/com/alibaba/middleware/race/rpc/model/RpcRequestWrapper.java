package com.alibaba.middleware.race.rpc.model;

import com.alibaba.middleware.race.rpc.api.codec.Serializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Dawnwords on 2015/7/31.
 */
public class RpcRequestWrapper implements SerializeWrapper<RpcRequest> {
    private long id;
    private byte[] methodName;
    private byte[] version;
    private byte[][] arguments;
    private byte[][] propKeys;
    private byte[][] propVals;

    @Override
    public RpcRequest deserialize(Serializer serializer) {
        ArrayList<Object> arguments = new ArrayList<Object>();
        if (this.arguments != null) {
            for (byte[] arg : this.arguments) {
                arguments.add(serializer.decode(arg));
            }
        }
        HashMap<String, Object> context = new HashMap<String, Object>();
        if (propKeys != null) {
            int mapSize = propKeys.length;
            for (int i = 0; i < mapSize; i++) {
                String key = (String) serializer.decode(propKeys[i]);
                Object val = serializer.decode(propVals[i]);
                context.put(key, val);
            }
        }

        return new RpcRequest()
                .id(id)
                .methodName((String) serializer.decode(methodName))
                .version((String) serializer.decode(version))
                .arguments(arguments.toArray())
                .context(context);
    }

    @Override
    public SerializeWrapper<RpcRequest> serialize(RpcRequest request, Serializer serializer) {
        this.id = request.id();
        this.methodName = serializer.encode(request.methodName());
        this.version = serializer.encode(request.version());
        Object[] requestArgs = request.arguments();
        if (requestArgs != null) {
            ArrayList<byte[]> arguments = new ArrayList<byte[]>();
            for (Object arg : requestArgs) {
                arguments.add(serializer.encode(arg));
            }
            this.arguments = arguments.toArray(new byte[arguments.size()][]);
        }

        Map<String, Object> context = request.context();
        if (context != null) {
            ArrayList<byte[]> propKeys = new ArrayList<byte[]>();
            ArrayList<byte[]> propVals = new ArrayList<byte[]>();
            for (String key : context.keySet()) {
                propKeys.add(serializer.encode(key));
                propVals.add(serializer.encode(context.get(key)));
            }
            int contextSize = context.size();
            this.propKeys = propKeys.toArray(new byte[contextSize][]);
            this.propVals = propVals.toArray(new byte[contextSize][]);
        }
        return this;
    }

    @Override
    public String toString() {
        return "RpcRequestWrapper{" +
                "id=" + id +
                ", methodName=" + (methodName == null ? null : new String(methodName)) +
                ", version=" + (version == null ? null : new String(version)) +
                ", arguments=" + (arguments == null ? null : arguments.length) +
                ", propKeys=" + (propKeys == null ? null : propKeys.length) +
                ", propVals=" + (propVals == null ? null : propVals.length) +
                '}';
    }
}
