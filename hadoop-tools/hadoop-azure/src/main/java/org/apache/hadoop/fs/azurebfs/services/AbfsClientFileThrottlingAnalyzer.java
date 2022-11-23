package org.apache.hadoop.fs.azurebfs.services;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

public class AbfsClientFileThrottlingAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsClientFileThrottlingAnalyzer.class);
  private static final int DEFAULT_ANALYSIS_PERIOD_MS = 30 * 1000;
  private static final int MIN_ANALYSIS_PERIOD_MS = 1000;
  private static final int MAX_ANALYSIS_PERIOD_MS = 30000;
  private static final double MIN_ACCEPTABLE_ERROR_PERCENTAGE = .1;
  private static final double MAX_EQUILIBRIUM_ERROR_PERCENTAGE = 1;
  private static final double RAPID_SLEEP_DECREASE_FACTOR = .75;
  private static final double RAPID_SLEEP_DECREASE_TRANSITION_PERIOD_MS = 150
      * 1000;
  private static final double SLEEP_DECREASE_FACTOR = .975;
  private static final double SLEEP_INCREASE_FACTOR = 1.05;
  private int analysisPeriodMs;

  private volatile int sleepDuration = 0;
  private long consecutiveNoErrorCount = 0;
  private String name = null;
  private Timer timer = null;
  private AtomicReference<AbfsClientFileThrottlingAnalyzer.AbfsOperationMetrics>
      blobMetrics = null;

  private static Map<String, AbfsClientFileThrottlingAnalyzer> fileThrottlingAnalyzerMap = new ConcurrentHashMap<>();

  private Long lastUsed = new Date().toInstant().toEpochMilli();

  static Thread garbageCleaner = new Thread(() -> {
    while(true) {
      for(String path : fileThrottlingAnalyzerMap.keySet()) {
        AbfsClientFileThrottlingAnalyzer analyzer = fileThrottlingAnalyzerMap.get(path);
        if(analyzer.lastUsed <= (new Date().toInstant().toEpochMilli() - 60*1000l)) {
          //kill it;
          if(analyzer.timer != null) {
            analyzer.timer.cancel();
          }
          fileThrottlingAnalyzerMap.remove(path);
        }
      }
      try {
        Thread.sleep(10000l);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  });

  private AbfsClientFileThrottlingAnalyzer() {
    // hide default constructor
  }

  /**
   * Creates an instance of the <code>AbfsClientThrottlingAnalyzer</code> class with
   * the specified name.
   *
   * @param name a name used to identify this instance.
   * @throws IllegalArgumentException if name is null or empty.
   */
  AbfsClientFileThrottlingAnalyzer(String name, String path) throws IllegalArgumentException {
    this(name, DEFAULT_ANALYSIS_PERIOD_MS);
    fileThrottlingAnalyzerMap.put(path, this);
  }

  public static AbfsClientFileThrottlingAnalyzer getAnalyzer(String path) {
    return fileThrottlingAnalyzerMap.get(path);
  }

  /**
   * Creates an instance of the <code>AbfsClientThrottlingAnalyzer</code> class with
   * the specified name and period.
   *
   * @param name   A name used to identify this instance.
   * @param period The frequency, in milliseconds, at which metrics are
   *               analyzed.
   * @throws IllegalArgumentException If name is null or empty.
   *                                  If period is less than 1000 or greater than 30000 milliseconds.
   */
  AbfsClientFileThrottlingAnalyzer(String name, int period)
      throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotEmpty(name),
        "The argument 'name' cannot be null or empty.");
    Preconditions.checkArgument(
        period >= MIN_ANALYSIS_PERIOD_MS && period <= MAX_ANALYSIS_PERIOD_MS,
        "The argument 'period' must be between 1000 and 30000.");
    this.name = name;
    this.analysisPeriodMs = period;
    this.blobMetrics = new AtomicReference<AbfsClientFileThrottlingAnalyzer.AbfsOperationMetrics>(
        new AbfsClientFileThrottlingAnalyzer.AbfsOperationMetrics(System.currentTimeMillis()));
    this.timer = new Timer(
        String.format("abfs-timer-client-throttling-analyzer-%s", name), true);
    this.timer.schedule(new AbfsClientFileThrottlingAnalyzer.TimerTaskImpl(),
        analysisPeriodMs,
        analysisPeriodMs);
  }

  /**
   * Updates metrics with results from the current storage operation.
   *
   * @param count             The count of bytes transferred.
   * @param isFailedOperation True if the operation failed; otherwise false.
   */
  public void addBytesTransferred(long count, boolean isFailedOperation) {
    AbfsClientFileThrottlingAnalyzer.AbfsOperationMetrics metrics = blobMetrics.get();
    if (isFailedOperation) {
      metrics.bytesFailed.addAndGet(count);
      metrics.operationsFailed.incrementAndGet();
    } else {
      metrics.bytesSuccessful.addAndGet(count);
      metrics.operationsSuccessful.incrementAndGet();
    }
  }

  /**
   * Suspends the current storage operation, as necessary, to reduce throughput.
   * @return true if Thread sleeps(Throttling occurs) else false.
   */
  public int suspendTime() {
    int duration = sleepDuration;
    LOG.info("sleep duration for prefetch: " + duration);
    return duration;
  }

  public void updateLMT() {
    lastUsed = new Date().toInstant().toEpochMilli();
  }

  @VisibleForTesting
  int getSleepDuration() {
    return sleepDuration;
  }

  private int analyzeMetricsAndUpdateSleepDuration(AbfsClientFileThrottlingAnalyzer.AbfsOperationMetrics metrics,
      int sleepDuration) {
    final double percentageConversionFactor = 100;
    double bytesFailed = metrics.bytesFailed.get();
    double bytesSuccessful = metrics.bytesSuccessful.get();
    double operationsFailed = metrics.operationsFailed.get();
    double operationsSuccessful = metrics.operationsSuccessful.get();
    double errorPercentage = (bytesFailed <= 0)
        ? 0
        : (percentageConversionFactor
            * bytesFailed
            / (bytesFailed + bytesSuccessful));
    long periodMs = metrics.endTime - metrics.startTime;

    double newSleepDuration;

    if (errorPercentage < MIN_ACCEPTABLE_ERROR_PERCENTAGE) {
      ++consecutiveNoErrorCount;
      // Decrease sleepDuration in order to increase throughput.
      double reductionFactor =
          (consecutiveNoErrorCount * analysisPeriodMs
              >= RAPID_SLEEP_DECREASE_TRANSITION_PERIOD_MS)
              ? RAPID_SLEEP_DECREASE_FACTOR
              : SLEEP_DECREASE_FACTOR;

      newSleepDuration = sleepDuration * reductionFactor;
    } else if (errorPercentage < MAX_EQUILIBRIUM_ERROR_PERCENTAGE) {
      // Do not modify sleepDuration in order to stabilize throughput.
      newSleepDuration = sleepDuration;
    } else {
      // Increase sleepDuration in order to minimize error rate.
      consecutiveNoErrorCount = 0;

      // Increase sleep duration in order to reduce throughput and error rate.
      // First, calculate target throughput: bytesSuccessful / periodMs.
      // Next, calculate time required to send *all* data (assuming next period
      // is similar to previous) at the target throughput: (bytesSuccessful
      // + bytesFailed) * periodMs / bytesSuccessful. Next, subtract periodMs to
      // get the total additional delay needed.
      double additionalDelayNeeded = 5 * analysisPeriodMs;
      if (bytesSuccessful > 0) {
        additionalDelayNeeded = (bytesSuccessful + bytesFailed)
            * periodMs
            / bytesSuccessful
            - periodMs;
      }

      // amortize the additional delay needed across the estimated number of
      // requests during the next period
      newSleepDuration = additionalDelayNeeded
          / (operationsFailed + operationsSuccessful);

      final double maxSleepDuration = analysisPeriodMs;
      final double minSleepDuration = sleepDuration * SLEEP_INCREASE_FACTOR;

      // Add 1 ms to avoid rounding down and to decrease proximity to the server
      // side ingress/egress limit.  Ensure that the new sleep duration is
      // larger than the current one to more quickly reduce the number of
      // errors.  Don't allow the sleep duration to grow unbounded, after a
      // certain point throttling won't help, for example, if there are far too
      // many tasks/containers/nodes no amount of throttling will help.
      newSleepDuration = Math.max(newSleepDuration, minSleepDuration) + 1;
      newSleepDuration = Math.min(newSleepDuration, maxSleepDuration);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format(
          "%5.5s, %10d, %10d, %10d, %10d, %6.2f, %5d, %5d, %5d",
          name,
          (int) bytesFailed,
          (int) bytesSuccessful,
          (int) operationsFailed,
          (int) operationsSuccessful,
          errorPercentage,
          periodMs,
          (int) sleepDuration,
          (int) newSleepDuration));
    }

    return (int) newSleepDuration;
  }

  /**
   * Timer callback implementation for periodically analyzing metrics.
   */
  class TimerTaskImpl extends TimerTask {
    private AtomicInteger doingWork = new AtomicInteger(0);

    /**
     * Periodically analyzes a snapshot of the blob storage metrics and updates
     * the sleepDuration in order to appropriately throttle storage operations.
     */
    @Override
    public void run() {
      boolean doWork = false;
      try {
        doWork = doingWork.compareAndSet(0, 1);

        // prevent concurrent execution of this task
        if (!doWork) {
          return;
        }

        long now = System.currentTimeMillis();
        if (now - blobMetrics.get().startTime >= analysisPeriodMs) {
          AbfsClientFileThrottlingAnalyzer.AbfsOperationMetrics oldMetrics = blobMetrics.getAndSet(
              new AbfsClientFileThrottlingAnalyzer.AbfsOperationMetrics(now));
          oldMetrics.endTime = now;
          sleepDuration = analyzeMetricsAndUpdateSleepDuration(oldMetrics,
              sleepDuration);
        }
      } finally {
        if (doWork) {
          doingWork.set(0);
        }
      }
    }
  }

  /**
   * Stores Abfs operation metrics during each analysis period.
   */
  static class AbfsOperationMetrics {
    private AtomicLong bytesFailed;
    private AtomicLong bytesSuccessful;
    private AtomicLong operationsFailed;
    private AtomicLong operationsSuccessful;
    private long endTime;
    private long startTime;

    AbfsOperationMetrics(long startTime) {
      this.startTime = startTime;
      this.bytesFailed = new AtomicLong();
      this.bytesSuccessful = new AtomicLong();
      this.operationsFailed = new AtomicLong();
      this.operationsSuccessful = new AtomicLong();
    }
  }
}
