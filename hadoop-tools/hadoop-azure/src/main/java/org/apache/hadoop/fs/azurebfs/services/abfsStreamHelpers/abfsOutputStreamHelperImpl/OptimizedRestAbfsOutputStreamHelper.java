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

package org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.abfsOutputStreamHelperImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsConnectionMode;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.AbfsOutputStreamHelper;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.exceptions.BlockHelperException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class OptimizedRestAbfsOutputStreamHelper
    implements AbfsOutputStreamHelper {

  private AbfsOutputStreamHelper nextHelper;

  private AbfsOutputStreamHelper prevHelper;

  @Override
  public Boolean isAbfsSessionRequired() {
    return true;
  }

  private static final Logger LOG = LoggerFactory.getLogger(
      OptimizedRestAbfsOutputStreamHelper.class);

  public OptimizedRestAbfsOutputStreamHelper(AbfsOutputStreamHelper abfsInputStreamHelper) {
    nextHelper = null;
    prevHelper = abfsInputStreamHelper;
  }

  @Override
  public boolean shouldGoNext(final AbfsOutputStreamContext abfsOutputStreamContext) {
    return false;
  }

  @Override
  public AbfsRestOperation operate(final String path,
      final byte[] buffer,
      final AppendRequestParameters reqParams,
      final String cachedSasToken,
      final TracingContext tracingContext, final AbfsClient abfsClient)
      throws AzureBlobFileSystemException {
    try {
      return abfsClient.append(path, buffer, reqParams, cachedSasToken,
          tracingContext);
    } catch (AzureBlobFileSystemException ex) {
      LOG.debug("Fallback: From OptimizedREST to Vanilla REST");
      tracingContext.setConnectionMode(
          AbfsConnectionMode.REST_CONN);
      reqParams.getAbfsSessionData()
          .setConnectionMode(
              AbfsConnectionMode.REST_CONN);
      throw new BlockHelperException(ex);
    }
  }

  @Override
  public void setNextAsValid() {
  }

  @Override
  public AbfsOutputStreamHelper getNext() {
    return nextHelper;
  }

  @Override
  public AbfsOutputStreamHelper getBack() {
    return prevHelper;
  }

  @Override
  public void setNextAsInvalid() {
    nextHelper = null;
  }
}
