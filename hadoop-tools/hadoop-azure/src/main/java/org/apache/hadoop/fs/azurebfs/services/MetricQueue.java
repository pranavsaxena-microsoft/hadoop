package org.apache.hadoop.fs.azurebfs.services;

import java.util.Date;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class MetricQueue {
  private static Map<String, Queue<Long>> accountConnTimeQueueMap = new ConcurrentHashMap();
  private static AtomicLong lastTimeInternalGCUse = new AtomicLong(new Date().toInstant().toEpochMilli());
  private static Map<String, Long> accountLastUse = new ConcurrentHashMap<>();

  private MetricQueue() {

  }

  public static void enqueueConnTime(String host, Long time,
      final AbfsRestOperationType operationType) {
    final String key = getKey(host, operationType);
    accountConnTimeQueueMap.putIfAbsent(key, new ConcurrentLinkedQueue<Long>());
    accountConnTimeQueueMap.get(key).add(time);
    commonCode(key);
  }

  private static String getKey(String host, AbfsRestOperationType operationType) {
    return host + "_" + operationType;
  }

  private static void commonCode(final String key) {
    accountLastUse.put(key, new Date().toInstant().toEpochMilli());
  }

  public static Long dequeueConnTime(final String host, final AbfsRestOperationType operationType) {
    final String key = getKey(host, operationType);
    commonCode(key);
    Queue<Long> queue = accountConnTimeQueueMap.get(key);
    if(queue == null) {
      return null;
    }
    return accountConnTimeQueueMap.get(key).poll();
  }
}
