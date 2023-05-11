package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class ListBlobProducer {
  private final AbfsClient client;
  private final ListBlobQueue listBlobQueue;
  private final String src;
  private final TracingContext tracingContext;
  private String nextMarker;

  public ListBlobProducer(final String src, final AbfsClient abfsClient,
      final ListBlobQueue listBlobQueue, final String initNextMarker, TracingContext tracingContext) {
    this.src = src;
    this.client = abfsClient;
    this.tracingContext = tracingContext;
    this.listBlobQueue = listBlobQueue;
    this.nextMarker = initNextMarker;
    new Thread(() -> {
      while(true) {
        if(listBlobQueue.getConsumerLag() > 7000) {
          continue;
        }
        AbfsRestOperation op = null;
        try {
          op = client.getListBlobs(nextMarker, src, null, tracingContext);
        } catch (AzureBlobFileSystemException ex) {
          listBlobQueue.setFailed(ex);
          throw new RuntimeException(ex);
        }
        BlobList blobList = op.getResult().getBlobList();
        nextMarker = blobList.getNextMarker();
        listBlobQueue.enqueue(blobList);
        if(nextMarker == null) {
          listBlobQueue.complete();
          break;
        }
      }
    }).start();
  }
}