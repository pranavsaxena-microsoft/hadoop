package org.apache.hadoop.fs.azurebfs.services;

import java.util.Date;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class MetricQueue {
  private static Map<String, Queue<Long>> accountConnTimeQueueMap = new ConcurrentHashMap();
  private static Long lastTimeInternalGCUse = new Date().toInstant().toEpochMilli();
  private static Map<String, Long> accountLastUse = new ConcurrentHashMap<>();

  private final static Long GC_STRIKE_PERIOD = 5 * 60 * 1_000L;

  private final static Long MAX_UNUSED_TIME = 10 * 60 * 1_000L;
  private MetricQueue() {

  }

  public static void enqueueConnTime(String host, Long time,
      final AbfsRestOperationType operationType) {
    final String key = getKey(host, operationType);
    commonCode(key);
    accountConnTimeQueueMap.putIfAbsent(key, new ConcurrentLinkedQueue<Long>());
    Queue<Long> queue = accountConnTimeQueueMap.get(key);
    /*
    * Queue can be null. There are two scenarios that can happen:
    * 1. commonCode() from this thread is run and before it comes map.get(),
    * another thread has started GC. That run of GC should not have any affect
    * on the current key because commonCode would have refreshed the time.
    *
    * 2. GC() from other thread has started before commonCode. Even if this key
    * is removed in GC, putIfAbsent will create a new queue.
    *
    * 3. GC() from other thread has started and it has picked that it has to
    * remove this key, context-switching happens and this thread moves past the
    * putIfAbsent method. Context-switching happens again and the GC running thread
    * gets CPU, it will delete the mapping.
    * */
    if(queue == null) {
      queue = new ConcurrentLinkedQueue<>();
      accountConnTimeQueueMap.put(key, queue);
    }
    queue.add(time);
  }

  private static String getKey(String host, AbfsRestOperationType operationType) {
    return host + "_" + operationType;
  }

  private static void commonCode(final String key) {
    final Long currentEpochMilli = new Date().toInstant().toEpochMilli();
    accountLastUse.put(key, currentEpochMilli);
    if((currentEpochMilli - lastTimeInternalGCUse > GC_STRIKE_PERIOD)) {
      runGC(currentEpochMilli);
    }
  }

  private synchronized static void runGC(final Long currentEpochMilli) {
    if((currentEpochMilli - lastTimeInternalGCUse <= GC_STRIKE_PERIOD)) {
      return;
    }
    lastTimeInternalGCUse = currentEpochMilli;
    for(String hostOperationKey : accountLastUse.keySet()) {
      if(currentEpochMilli - accountLastUse.get(hostOperationKey) > MAX_UNUSED_TIME) {
        accountLastUse.remove(hostOperationKey);
        accountConnTimeQueueMap.remove(hostOperationKey);
      }
    }
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
