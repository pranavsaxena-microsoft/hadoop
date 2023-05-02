package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

public class RenameListBlobConsumer {
  private final RenameListBlobQueue renameListBlobQueue;
  public RenameListBlobConsumer(final RenameListBlobQueue renameListBlobQueue) {
    this.renameListBlobQueue = renameListBlobQueue;
  }

  public BlobList consume() throws AzureBlobFileSystemException {
    if(renameListBlobQueue.getFailed()) {
      throw renameListBlobQueue.getException();
    }
    return renameListBlobQueue.dequeue();
  }

  public Boolean isCompleted() {
    return renameListBlobQueue.getIsCompleted();
  }
}
