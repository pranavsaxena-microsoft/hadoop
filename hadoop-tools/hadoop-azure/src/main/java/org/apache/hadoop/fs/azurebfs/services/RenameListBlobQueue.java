package org.apache.hadoop.fs.azurebfs.services;

import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

public class RenameListBlobQueue {

  private final Queue<BlobList> blobLists = new ArrayDeque<>();
  private int totalProduced = 0;
  private int totalConsumed = 0;

  private Boolean failed = false;
  private Boolean isCompleted = false;

  private AzureBlobFileSystemException failureFromProducer;

  void setFailed(AzureBlobFileSystemException failure) {
    failed = true;
    failureFromProducer = failure;
  }

  Boolean getFailed() {
    return failed;
  }

  void complete() {
    isCompleted = true;
  }

  Boolean getIsCompleted() {
    return isCompleted;
  }

  AzureBlobFileSystemException getException() {
    return failureFromProducer;
  }

  public synchronized void enqueue(BlobList blobList) {
    blobLists.add(blobList);
    totalProduced += blobList.getBlobPropertyList().size();
  }

  public synchronized BlobList dequeue() {
    BlobList blobList = blobLists.poll();
    if (blobList != null) {
      totalConsumed += blobList.getBlobPropertyList().size();
    }
    return blobList;
  }

  public synchronized int getConsumerLag() {
    return totalProduced - totalConsumed;
  }
}
