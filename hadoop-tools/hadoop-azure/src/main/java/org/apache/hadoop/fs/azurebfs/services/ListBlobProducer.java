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

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class ListBlobProducer {

  private final AbfsClient client;

  private final ListBlobQueue listBlobQueue;

  private final String src;

  private final TracingContext tracingContext;

  private String nextMarker;

  public ListBlobProducer(final String src,
      final AbfsClient abfsClient,
      final ListBlobQueue listBlobQueue,
      final String initNextMarker,
      TracingContext tracingContext) {
    this.src = src;
    this.client = abfsClient;
    this.tracingContext = tracingContext;
    this.listBlobQueue = listBlobQueue;
    this.nextMarker = initNextMarker;
    new Thread(() -> {
      while (true) {
        if (listBlobQueue.getConsumerLag() >= client.getAbfsConfiguration()
            .getMaximumConsumerLag()) {
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
        if (nextMarker == null) {
          listBlobQueue.complete();
          break;
        }
      }
    }).start();
  }
}
