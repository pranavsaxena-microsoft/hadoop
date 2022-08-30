package org.apache.hadoop.fs.azurebfs.services;

public class AbfsInputStreamRequestContext {
  private Long startOffset;
  private Long len;

  private int maxReadAhead = 0;

  public Long getStartOffset() {
    return startOffset;
  }

  public void setStartOffset(final Long startOffset) {
    this.startOffset = startOffset;
  }

  public Long getLen() {
    return len;
  }

  public void setLen(final Long len) {
    this.len = len;
  }

  public int getMaxReadAhead() {
    return maxReadAhead;
  }

  public void setMaxReadAhead(final int maxReadAhead) {
    this.maxReadAhead = maxReadAhead;
  }
}
