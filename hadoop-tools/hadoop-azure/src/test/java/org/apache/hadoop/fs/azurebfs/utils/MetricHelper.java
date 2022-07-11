package org.apache.hadoop.fs.azurebfs.utils;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MetricHelper {

    private static final Long ONE_MINUTE_DIFF = 60 * 1000l;
    private static Queue<MetricUnit> metricUnitQueue = new ArrayDeque<>();

    private static ScheduledFuture scheduledFuture;

    private static void stopPlot() {
        scheduledFuture.cancel(true);
    }

    public static void push(Long latency) {
        metricUnitQueue.add(new MetricUnit(latency, new Date().toInstant().toEpochMilli()));
    }

    public static void startPlot() {
        scheduledFuture = new ScheduledThreadPoolExecutor(1).schedule(() -> {
            List<MetricUnit> filteredMetricUnits;
            if(metricUnitQueue.size() == 0) {
                return;
            }
            while(metricUnitQueue.poll().latency >= (new Date().toInstant().toEpochMilli() - ONE_MINUTE_DIFF)) {
                metricUnitQueue.remove();
            }
            filteredMetricUnits = new ArrayList<>(metricUnitQueue);
            filteredMetricUnits.sort((m1, m2) -> {
                return (int) (m1.latency - m2.latency);
            });
            plot(filteredMetricUnits.get((int)((filteredMetricUnits.size() - 1)*0.99)));
        }, ONE_MINUTE_DIFF/2, TimeUnit.MILLISECONDS);
    }

    private static void plot(MetricUnit metricUnit) {

    }

    static class MetricUnit {
        Long time;
        Long latency;

        public MetricUnit(Long time, Long latency) {
            this.time = time;
            this.latency = latency;
        }
    }
}
