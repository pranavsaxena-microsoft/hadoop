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

import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

public class ListBlobQueue {

  private final Queue<BlobList> blobLists = new ArrayDeque<>();

  private int totalProduced = 0;

  private int totalConsumed = 0;

  private Boolean isCompleted = false;

  private AzureBlobFileSystemException failureFromProducer;

  /**
   * Since, Producer just spawns a thread and there are no public method for the
   * class. Keeping its address in this object will prevent accidental GC close
   * on the producer object.
   */
  private ListBlobProducer producer;

  public ListBlobQueue(BlobList initBlobList) {
    if (initBlobList != null) {
      enqueue(initBlobList);
    }
  }

  void setProducer(ListBlobProducer producer) {
    if (this.producer == null) {
      this.producer = producer;
    }
  }

  void setFailed(AzureBlobFileSystemException failure) {
    failureFromProducer = failure;
  }

  public void complete() {
    isCompleted = true;
  }

  public Boolean getIsCompleted() {
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
