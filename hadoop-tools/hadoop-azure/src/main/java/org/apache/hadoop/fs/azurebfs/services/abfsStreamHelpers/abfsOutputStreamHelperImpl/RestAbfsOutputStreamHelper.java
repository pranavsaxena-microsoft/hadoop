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

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.AbfsOutputStreamHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class RestAbfsOutputStreamHelper implements AbfsOutputStreamHelper {

  private AbfsOutputStreamHelper nextHelper;
  private Boolean isNextHelperIsValid = true;

  public RestAbfsOutputStreamHelper() {
    nextHelper = new OptimizedRestAbfsOutputStreamHelper(this);
  }

  @Override
  public AbfsOutputStreamHelper getNext() {
    return nextHelper;
  }

  @Override
  public AbfsOutputStreamHelper getBack() {
    return null;
  }

  @Override
  public void setNextAsInvalid() {
    isNextHelperIsValid = false;
  }

  @Override
  public boolean shouldGoNext(final AbfsOutputStreamContext abfsOutputStreamContext) {
    return (abfsOutputStreamContext.isDefaultConnectionOnOptimizedRest()
        && nextHelper != null && isNextHelperIsValid);
  }

  @Override
  public void setNextAsValid() {
    isNextHelperIsValid = true;
  }

  @Override
  public Boolean isAbfsSessionRequired() {
    return false;
  }

  @Override
  public AbfsRestOperation operate(final String path,
      final byte[] buffer,
      final AppendRequestParameters reqParams,
      final String cachedSasToken,
      final TracingContext tracingContext, final AbfsClient abfsClient) throws AzureBlobFileSystemException {
    return abfsClient.append(path, buffer, reqParams, cachedSasToken, tracingContext);
  }
}
