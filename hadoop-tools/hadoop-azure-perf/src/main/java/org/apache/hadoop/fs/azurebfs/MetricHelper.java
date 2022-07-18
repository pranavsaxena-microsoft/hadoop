package org.apache.hadoop.fs.azurebfs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;

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

    public static void startPlot(String path) {

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
                        plot(filteredMetricUnits, path);
                    }
                } catch (Exception e) {
                    int a;
                    a=1;
                }
            }
        }).start();
    }

    private static MetricUnit getPercentile(List<MetricUnit> metricUnitList, Double percentileVal) {
        if(metricUnitList == null || metricUnitList.size() == 0) {
            return null;
        }
        return metricUnitList.get((int) ((metricUnitList.size() - 1) * percentileVal));
    }

    private static void plot(List<MetricUnit> metricUnitList, String path) throws IOException {
        //"/home/pranav/Desktop/metrics.csv"
        if(metricUnitList == null | metricUnitList.size() == 0) {
            return;
        }
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path, true));
        int sampleSize = metricUnitList.size();
        MetricUnit percentile99 = getPercentile(metricUnitList, 0.99);
        MetricUnit percentile90 = getPercentile(metricUnitList, 0.9);
        MetricUnit percentile50 = getPercentile(metricUnitList, 0.5);
        bufferedWriter.append(sampleSize + "," + percentile50.latency + "," + percentile90.latency
                + "," + percentile99.latency + "\n");
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
