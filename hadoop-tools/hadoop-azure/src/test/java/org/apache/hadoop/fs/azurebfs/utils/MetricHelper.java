package org.apache.hadoop.fs.azurebfs.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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
        metricUnitQueue.add(new MetricUnit(new Date().toInstant().toEpochMilli(), latency));
    }

    public static void startPlot() {

        new Thread(() -> {
            while(true) {
                try {
                    Thread.sleep(ONE_MINUTE_DIFF / 2);
                    List<MetricUnit> filteredMetricUnits;
                    if (metricUnitQueue.size() == 0) {
                        return;
                    }
                    while (metricUnitQueue.size() > 0 && metricUnitQueue.peek().time < (new Date().toInstant().toEpochMilli() - ONE_MINUTE_DIFF)) {
                        metricUnitQueue.remove();
                    }
                    filteredMetricUnits = new ArrayList<>(metricUnitQueue);
                    filteredMetricUnits.sort((m1, m2) -> {
                        return (int) (m1.latency - m2.latency);
                    });
                    if(metricUnitQueue.size() > 0) {
                        plot(filteredMetricUnits.size(), filteredMetricUnits.get((int) ((filteredMetricUnits.size() - 1) * 0.99)));
                    }
                } catch (Exception e) {
                    int a;
                    a=1;
                }
            }
        }).start();
    }

    private static void plot(int sampleSize, MetricUnit metricUnit) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("/home/pranav/Desktop/metrics.csv", true));
        bufferedWriter.append(sampleSize + "," + metricUnit.latency + "\n");
        bufferedWriter.close();
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
