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

import java.time.OffsetDateTime;
import java.util.Base64;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.CONTENT_LENGTH;

public class AbfsFastpathSession extends AbfsSession {

  private AbfsFastpathSessionData fastpathSessionData;

  public AbfsFastpathSession(final IO_SESSION_SCOPE scope,
      final AbfsClient client,
      final String path,
      final String eTag,
      TracingContext tracingContext) {
    super(scope, client, path, eTag, tracingContext);

    if (isValid()) {
      fetchFastpathFileHandle();
    }

  }

  @VisibleForTesting
  protected boolean fetchFastpathFileHandle() {
    try {
      AbfsRestOperation op = executeFastpathOpen();
      String fileHandle
          = ((AbfsFastpathConnection) op.getResult()).getFastpathFileHandle();
      updateFastpathFileHandle(fileHandle);
      return true;
    } catch (AzureBlobFileSystemException e) {
      LOG.debug("Fastpath open failed with {}", e);
      updateConnectionMode(AbfsConnectionMode.OPTIMIZED_REST_ON_FASTPATH_CONN_FAILURE);
    }

    return false;
  }

  @VisibleForTesting
  protected AbfsRestOperation executeFastpathOpen()
      throws AzureBlobFileSystemException {
    return client.fastPathOpen(path, eTag, fastpathSessionData, tracingContext);
  }

  @Override
  protected AbfsSessionData createSessionDataInstance(final AbfsRestOperation op,
      AbfsConnectionMode mode) {
    AbfsSessionData baseSsnData = super.createSessionDataInstance(op, mode);
    byte[] buffer = new byte[Integer.parseInt(
        op.getResult().getResponseHeader(CONTENT_LENGTH))];
    op.getResult().getResponseContentBuffer(buffer);
    String fastpathSessionToken = Base64.getEncoder().encodeToString(buffer);
    fastpathSessionData = new AbfsFastpathSessionData(baseSsnData,
        fastpathSessionToken);
    return fastpathSessionData;
  }

  protected AbfsSessionData getSessionDataCopy() {
    LOG.debug("AbfsFasthSession - getASessionCopy");
    return fastpathSessionData.getAClone();
  }

  @Override
  public void close() {
    if ((fastpathSessionData != null)
            && (fastpathSessionData.getFastpathFileHandle() != null)) {
      try {
        executeFastpathClose();
      } catch (AzureBlobFileSystemException e) {
        LOG.debug("Fastpath handle close failed - {} - {}",
            fastpathSessionData.getFastpathFileHandle(), e);
      }
    }

    super.close();
  }

  private void updateFastpathFileHandle(String fileHandle) {
    rwLock.writeLock().lock();
    try {
      fastpathSessionData.setFastpathFileHandle(fileHandle);
      LOG.debug("Fastpath handled opened {}",
          fastpathSessionData.getFastpathFileHandle());
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @VisibleForTesting
  protected AbfsRestOperation executeFastpathClose()
      throws AzureBlobFileSystemException {
    return client.fastPathClose(path, eTag, fastpathSessionData,
        tracingContext);
  }


}
