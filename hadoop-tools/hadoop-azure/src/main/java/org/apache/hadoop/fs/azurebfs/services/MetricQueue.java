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

  public static void enqueueConnTime(String host, Long time) {
    accountConnTimeQueueMap.putIfAbsent(host, new ConcurrentLinkedQueue<Long>());
    accountConnTimeQueueMap.get(host).add(time);
    commonCode(host);
  }

  private static void commonCode(final String host) {
    accountLastUse.put(host, new Date().toInstant().toEpochMilli());
  }

  public static Long dequeueConnTime(final String host) {
    commonCode(host);
    Queue<Long> queue = accountConnTimeQueueMap.get(host);
    if(queue == null) {
      return null;
    }
    return accountConnTimeQueueMap.get(host).poll();
  }
}
