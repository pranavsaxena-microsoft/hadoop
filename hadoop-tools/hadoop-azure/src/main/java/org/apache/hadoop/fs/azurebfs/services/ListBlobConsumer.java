package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

public class ListBlobConsumer {

  private final ListBlobQueue listBlobQueue;

  public ListBlobConsumer(final ListBlobQueue listBlobQueue) {
    this.listBlobQueue = listBlobQueue;
  }

  public BlobList consume() throws AzureBlobFileSystemException {
    if (listBlobQueue.getException() != null) {
      throw listBlobQueue.getException();
    }
    return listBlobQueue.dequeue();
  }

  public Boolean isCompleted() {
    return listBlobQueue.getIsCompleted()
        && listBlobQueue.getConsumerLag() == 0;
  }
}