package org.apache.hadoop.fs.azurebfs.services;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class RenameListBlobProducer {
  private final AbfsClient client;
  private final RenameListBlobQueue renameListBlobQueue;
  private final String src;
  private final TracingContext tracingContext;
  private String nextMarker;

  public RenameListBlobProducer(final String src, final AbfsClient abfsClient,
      final RenameListBlobQueue renameListBlobQueue, TracingContext tracingContext) {
    this.src = src;
    this.client = abfsClient;
    this.tracingContext = tracingContext;
    this.renameListBlobQueue = renameListBlobQueue;
    new Thread(() -> {
      while(true) {
        if(renameListBlobQueue.getConsumerLag() > 7000) {
          continue;
        }
        AbfsRestOperation op = null;
        try {
          op = client.getListBlobs(nextMarker, src, null, tracingContext);
        } catch (AzureBlobFileSystemException ex) {
          renameListBlobQueue.setFailed(ex);
          throw new RuntimeException(ex);
        }
        BlobList blobList = op.getResult().getBlobList();
        nextMarker = blobList.getNextMarker();
        renameListBlobQueue.enqueue(blobList);
        if(nextMarker == null) {
          break;
        }
      }
    }).start();
  }
}
