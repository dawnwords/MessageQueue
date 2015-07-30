package com.alibaba.middleware.race.rpc.context;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangsheng.hs on 2015/4/8.
 */
public class RpcContext {

    private static final ThreadLocal<Map<String, Object>> propsTL = new ThreadLocal<Map<String, Object>>() {
        @Override
        public Map<String, Object> get() {
            Map<String, Object> props = super.get();
            if (props == null) {
                props = new HashMap<String, Object>();
                set(props);
            }
            return props;
        }
    };

    public static void addProp(String key, Object value) {
        propsTL.get().put(key, value);
    }

    public static void setProp(Map<String, Object> props) {
        propsTL.set(props);
    }

    public static Object getProp(String key) {
        return propsTL.get().get(key);
    }

    public static Map<String, Object> getProps() {
        return Collections.unmodifiableMap(propsTL.get());
    }
}
