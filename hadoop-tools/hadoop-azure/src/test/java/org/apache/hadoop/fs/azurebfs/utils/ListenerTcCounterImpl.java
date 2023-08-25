package org.apache.hadoop.fs.azurebfs.utils;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;

public class ListenerTcCounterImpl implements Listener {
  AtomicInteger counter = new AtomicInteger(0);
  @Override
  public void callTracingHeaderValidator(final String header,
      final TracingHeaderFormat format) {
    counter.incrementAndGet();
  }

  @Override
  public void updatePrimaryRequestID(final String primaryRequestID) {

  }

  @Override
  public Listener getClone() {
    ListenerTcCounterImpl listenerTcCounter = new ListenerTcCounterImpl();
    listenerTcCounter.setCounter(getCounter());
    return listenerTcCounter;
  }

  @Override
  public void setOperation(final FSOperationType operation) {

  }

  public AtomicInteger getCounter() {
    return counter;
  }

  public void setCounter(AtomicInteger counter) {
    this.counter = counter;
  }
}
