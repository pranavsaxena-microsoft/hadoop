/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;

import static org.apache.hadoop.fs.azurebfs.contracts.services.ReadBufferStatus.READ_FAILED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_BLOCKS_USED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_BYTES_DISCARDED;
import static org.apache.hadoop.fs.statistics.StreamStatisticNames.STREAM_READ_PREFETCH_BYTES_USED;

class ReadBuffer {

  private AbfsInputStream stream;
  private long offset;                   // offset within the file for the buffer
  private int length;                    // actual length, set after the buffer is filles
  private int requestedLength;           // requested length of the read
  private byte[] buffer;                 // the buffer itself
  private int bufferindex = -1;          // index in the buffers array in Buffer manager
  private ReadBufferStatus status;             // status of the buffer
  private CountDownLatch latch = null;   // signaled when the buffer is done reading, so any client
  // waiting on this buffer gets unblocked
  private TracingContext tracingContext;

  // fields to help with eviction logic
  private long timeStamp = 0;  // tick at which buffer became available to read
  private boolean isFirstByteConsumed = false;
  private boolean isLastByteConsumed = false;
  private boolean isAnyByteConsumed = false;

  private IOException errException = null;

  public AbfsInputStream getStream() {
    return stream;
  }

  public void setStream(AbfsInputStream stream) {
    this.stream = stream;
  }

  public void setTracingContext(TracingContext tracingContext) {
    this.tracingContext = tracingContext;
  }

  public TracingContext getTracingContext() {
    return tracingContext;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public int getRequestedLength() {
    return requestedLength;
  }

  public void setRequestedLength(int requestedLength) {
    this.requestedLength = requestedLength;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public void setBuffer(byte[] buffer) {
    this.buffer = buffer;
  }

  public int getBufferindex() {
    return bufferindex;
  }

  public void setBufferindex(int bufferindex) {
    this.bufferindex = bufferindex;
  }

  public IOException getErrException() {
    return errException;
  }

  public void setErrException(final IOException errException) {
    this.errException = errException;
  }

  public ReadBufferStatus getStatus() {
    return status;
  }

  /**
   * Update the read status.
   * If it was a read failure, the buffer index is set to -1;
   * @param status status
   */
  public void setStatus(ReadBufferStatus status) {
    this.status = status;
    if (status == READ_FAILED) {
      bufferindex = -1;
    }
  }

  public CountDownLatch getLatch() {
    return latch;
  }

  public void setLatch(CountDownLatch latch) {
    this.latch = latch;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.timeStamp = timeStamp;
  }

  public boolean isFirstByteConsumed() {
    return isFirstByteConsumed;
  }

  public void setFirstByteConsumed(boolean isFirstByteConsumed) {
    this.isFirstByteConsumed = isFirstByteConsumed;
  }

  public boolean isLastByteConsumed() {
    return isLastByteConsumed;
  }

  public void setLastByteConsumed(boolean isLastByteConsumed) {
    this.isLastByteConsumed = isLastByteConsumed;
  }

  public boolean isAnyByteConsumed() {
    return isAnyByteConsumed;
  }

  public void setAnyByteConsumed(boolean isAnyByteConsumed) {
    this.isAnyByteConsumed = isAnyByteConsumed;
  }

  @Override
  public String toString() {
    return super.toString() +
        "{ status=" + status +
        ", offset=" + offset +
        ", length=" + length +
        ", requestedLength=" + requestedLength +
        ", bufferindex=" + bufferindex +
        ", timeStamp=" + timeStamp +
        ", isFirstByteConsumed=" + isFirstByteConsumed +
        ", isLastByteConsumed=" + isLastByteConsumed +
        ", isAnyByteConsumed=" + isAnyByteConsumed +
        ", errException=" + errException +
        ", stream=" + stream.getStreamID() +
        ", stream closed=" + isStreamClosed() +
        '}';
  }

  /**
   * Is the stream closed.
   * @return stream closed status.
   */
  public boolean isStreamClosed() {
    return stream.isClosed();
  }

  /**
   * IOStatistics of stream.
   * @return the stream's IOStatisticsStore.
   */
  public IOStatisticsStore getStreamIOStatistics() {
    return stream.getIOStatistics();
  }

  /**
   * Start using the buffer.
   * Sets the byte consumption flags as appriopriate, then
   * updates the stream statistics with the use of this buffer.
   * @param offset offset in buffer where copy began
   * @param bytesCopied bytes copied.
   */
  public void dataConsumedByStream(int offset, int bytesCopied) {
    setAnyByteConsumed(true);
    if (offset == 0) {
      setFirstByteConsumed(true);
    }
    if (offset + bytesCopied == getLength()) {
      setLastByteConsumed(true);
    }
    IOStatisticsStore iostats = getStreamIOStatistics();
    iostats.incrementCounter(STREAM_READ_PREFETCH_BLOCKS_USED, 1);
    iostats.incrementCounter(STREAM_READ_PREFETCH_BYTES_USED, bytesCopied);
  }


  /**
   * The read completed
   * @param result read result
   * @param bytesActuallyRead the number of bytes actually read.
   * @param timestampMillis timestamp of completion
   */
  void readCompleted(
      final ReadBufferStatus result,
      final int bytesActuallyRead,
      final long timestampMillis) {
    setStatus(status);
    if (status == ReadBufferStatus.AVAILABLE) {
      setLength(bytesActuallyRead);
    } else {
      setLength(0);
    }
    setTimeStamp(timestampMillis);
  }

  /**
   * Has the read succeeded wth valid data
   * @return true if there is data from a successful read
   */
  boolean readSucceededWithData() {
    return status == ReadBufferStatus.AVAILABLE
        && getLength() > 0;
  }

  /**
   * The (completed) buffer was evicted; update stream statistics
   * as appropriate.
   */
  public void evicted() {
    if (getBufferindex() >= 0) {
      IOStatisticsStore iostats = getStreamIOStatistics();
      iostats.incrementCounter(STREAM_READ_PREFETCH_BYTES_DISCARDED, 1);
      iostats.incrementCounter(STREAM_READ_PREFETCH_BYTES_USED, getLength());
    }
  }

  /**
   * Release the buffer: update fields as appropriate.
   */
  void releaseBuffer() {
    setBuffer(null);
    setBufferindex(-1);
  }
}
