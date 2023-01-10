package org.apache.hadoop.fs.azurebfs;

public interface MockIntercept {
  public Exception throwException();
  public Object mockValue();
}
