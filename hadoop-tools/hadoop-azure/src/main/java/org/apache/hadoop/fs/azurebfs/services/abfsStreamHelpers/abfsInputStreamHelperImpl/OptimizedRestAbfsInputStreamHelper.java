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

package org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.abfsInputStreamHelperImpl;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsConnectionMode;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamRequestContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsOptimizedRestResponseHeaderBlock;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsSessionData;
import org.apache.hadoop.fs.azurebfs.services.ReadBufferManager;
import org.apache.hadoop.fs.azurebfs.services.ThreadBasedMessageQueue;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.AbfsInputStreamHelper;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.exceptions.BlockHelperException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimizedRestAbfsInputStreamHelper
    implements AbfsInputStreamHelper {

  private AbfsInputStreamHelper nextHelper;

  private AbfsInputStreamHelper prevHelper;

  private Boolean isNextHelperValid = true;

  @Override
  public void setNextAsValid() {
    isNextHelperValid = true;
  }

  @Override
  public Boolean isAbfsSessionRequired() {
    return true;
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      OptimizedRestAbfsInputStreamHelper.class);

  public OptimizedRestAbfsInputStreamHelper(AbfsInputStreamHelper abfsInputStreamHelper) {
    nextHelper = new FastpathRimbaudAbfsInputStreamHelper(this);
    prevHelper = abfsInputStreamHelper;
  }

  @Override
  public boolean shouldGoNext(final AbfsInputStreamContext abfsInputStreamContext) {
    return (abfsInputStreamContext.isDefaultConnectionOnFastpath()
        && nextHelper != null);
  }

  @Override
  public AbfsInputStreamHelper getNext() {
    return nextHelper;
  }

  @Override
  public AbfsInputStreamHelper getBack() {
    return prevHelper;
  }

  @Override
  public void setNextAsInvalid() {
    isNextHelperValid = false;
  }

  @Override
  public AbfsRestOperation operate(String path,
      byte[] bytes,
      String sasToken,
      ReadRequestParameters readRequestParameters,
      TracingContext tracingContext,
      AbfsClient abfsClient,
      final AbfsInputStreamRequestContext abfsInputStreamRequestContext)
      throws AzureBlobFileSystemException {
    try {
      Callable callable = new Callable() {
        private String uuid = UUID.randomUUID().toString();

        @Override
        public boolean equals(Object o) {
          return super.equals(o);
        }

        @Override
        public Object call() throws Exception {
          AbfsOptimizedRestResponseHeaderBlock
              abfsOptimizedRestResponseHeaderBlock
              = (AbfsOptimizedRestResponseHeaderBlock) ThreadBasedMessageQueue.getData(
              this);
          final AbfsSessionData currentSessionData
              = readRequestParameters.getAbfsSessionData();
          final AbfsSessionData sessionDataForNextRequest = new AbfsSessionData(
              abfsOptimizedRestResponseHeaderBlock.getSessionToken(),
              OffsetDateTime.parse(
                  abfsOptimizedRestResponseHeaderBlock.getSessionExpiry(),
                  DateTimeFormatter.RFC_1123_DATE_TIME),
              currentSessionData.getConnectionMode());
          abfsInputStreamRequestContext.setAbfsSessionData(
              sessionDataForNextRequest);

          Long nextSize = Math.min(
              abfsInputStreamRequestContext.getBufferSize(),
              (abfsInputStreamRequestContext.getLen()
                  + abfsInputStreamRequestContext.getCurrentOffset()) - (
                  readRequestParameters.getStoreFilePosition()
                      + readRequestParameters.getReadLength()));

          if (nextSize == 0) {
            if (abfsInputStreamRequestContext.getCurrentOffset()
                .compareTo(abfsInputStreamRequestContext.getStartOffset())
                != 0) {
              return null;
            }
            int readAheadCount
                = abfsInputStreamRequestContext.getMaxReadAhead();
            readAheadCount--;
            if (readAheadCount > 0) {
              nextSize = Math.min(abfsInputStreamRequestContext.getBufferSize(),
                  abfsInputStreamRequestContext.getContentLength() - (
                      readRequestParameters.getStoreFilePosition()
                          + readRequestParameters.getReadLength()));
              abfsInputStreamRequestContext.setMaxReadAhead(readAheadCount);
              abfsInputStreamRequestContext.setCurrentOffset(
                  readRequestParameters.getStoreFilePosition()
                      + readRequestParameters.getReadLength());
              abfsInputStreamRequestContext.setStartOffset(
                  readRequestParameters.getStoreFilePosition()
                      + readRequestParameters.getReadLength());
              abfsInputStreamRequestContext.setLen(nextSize);
              ReadBufferManager.getBufferManager()
                  .queueReadAhead(
                      abfsInputStreamRequestContext.getAbfsInputStream(),
                      readRequestParameters.getStoreFilePosition()
                          + readRequestParameters.getReadLength(),
                      nextSize.intValue(), tracingContext,
                      abfsInputStreamRequestContext);
            }
          } else {
            abfsInputStreamRequestContext.setCurrentOffset(
                readRequestParameters.getStoreFilePosition()
                    + readRequestParameters.getReadLength());
            abfsInputStreamRequestContext.setLen(
                abfsInputStreamRequestContext.getLen() - nextSize);
            ReadBufferManager.getBufferManager()
                .queueReadAhead(
                    abfsInputStreamRequestContext.getAbfsInputStream(),
                    readRequestParameters.getStoreFilePosition()
                        + readRequestParameters.getReadLength(),
                    nextSize.intValue(), tracingContext,
                    abfsInputStreamRequestContext);
          }
          return null;
        }
      };
      final AbfsRestOperation op = abfsClient.read(path, bytes, sasToken,
          readRequestParameters, tracingContext, callable);
      return op;
    } catch (AzureBlobFileSystemException e) {
      if (readRequestParameters.isOptimizedRestConnection()) {
        LOG.debug("Fallback: From OptimizedREST to Vanilla REST");
        tracingContext.setConnectionMode(
            AbfsConnectionMode.REST_CONN);
        readRequestParameters.getAbfsSessionData()
            .setConnectionMode(
                AbfsConnectionMode.REST_CONN);
        throw new BlockHelperException(e);
      }

      throw e;
    }
  }

  @Override
  public Boolean explicitPreFetchReadAllowed() {
    return false;
  }
}
