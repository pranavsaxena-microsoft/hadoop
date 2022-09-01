package org.apache.hadoop.fs.azurebfs.services;

public class AbfsInputStreamRequestContext {
  private Long startOffset;
  private Long currentOffset;
  //len is the length of the data requested
  private Long len;
  private Long bufferSize;
  private AbfsInputStream abfsInputStream;
  private AbfsSessionData abfsSessionData;
  //contentLength is the length of the total file
  private Long contentLength;

  private int maxReadAhead = 0;

  public Long getStartOffset() {
    return startOffset;
  }

  public Long getBufferSize() {
    return bufferSize;
  }

  public Long getContentLength() {
    return contentLength;
  }

  public void setContentLength(final Long contentLength) {
    this.contentLength = contentLength;
  }

  public AbfsInputStream getAbfsInputStream() {
    return abfsInputStream;
  }

  public void setAbfsInputStream(final AbfsInputStream abfsInputStream) {
    this.abfsInputStream = abfsInputStream;
  }

  public void setBufferSize(final Long bufferSize) {
    this.bufferSize = bufferSize;
  }

  public Long getCurrentOffset() {
    return currentOffset;
  }

  public void setCurrentOffset(final Long currentOffset) {
    this.currentOffset = currentOffset;
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

  public AbfsSessionData getAbfsSessionData() {
    return abfsSessionData;
  }

  public void setAbfsSessionData(final AbfsSessionData abfsSessionData) {
    this.abfsSessionData = abfsSessionData;
  }
}
