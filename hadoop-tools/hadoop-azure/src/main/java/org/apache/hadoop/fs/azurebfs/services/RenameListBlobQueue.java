package org.apache.hadoop.fs.azurebfs.services;

import java.util.List;

public class RenameListBlobQueue {

  private Boolean failed = false;

  void setFailed() {
    failed = true;
  }

  Boolean getFailed() {
    return failed;
  }

  public synchronized void enqueue(List<BlobProperty> blobPropertyList) {
  }

  public synchronized List<BlobProperty> dequeue() {
    return null;
  }

  public synchronized int getConsumerLag() {
    return 0;
  }
}
