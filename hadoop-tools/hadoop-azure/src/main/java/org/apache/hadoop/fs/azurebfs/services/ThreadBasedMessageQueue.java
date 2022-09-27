package org.apache.hadoop.fs.azurebfs.services;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class ThreadBasedMessageQueue {
    private static Map<Callable, Object> callableObjectMap= new HashMap<Callable, Object>();

    public static void push(Callable callable, Object obj) {
        callableObjectMap.put(callable, obj);
    }

    public  static Object getData(Callable callable) {
        Object obj = callableObjectMap.get(callable);
        removeObject(callable);
        return obj;
    }

    public static void removeObject(Callable callable) {
        callableObjectMap.remove(callable);
    }

}
