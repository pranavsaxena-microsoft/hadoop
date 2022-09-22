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
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamRequestContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.AbfsInputStreamHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class RestAbfsInputStreamHelper implements AbfsInputStreamHelper {

  private AbfsInputStreamHelper nextHelper;
  private Boolean isNextHelperValid = true;

  public RestAbfsInputStreamHelper() {
    nextHelper = new OptimizedRestAbfsInputStreamHelper(this);
  }

  @Override
  public boolean shouldGoNext(final AbfsInputStreamContext abfsInputStreamContext) {
    if (abfsInputStreamContext == null) {
      return false;
    }
    return (abfsInputStreamContext.isDefaultConnectionOnOptimizedRest()
        && nextHelper != null && isNextHelperValid);
  }

  @Override
  public void setNextAsValid() {
    isNextHelperValid = true;
  }

  @Override
  public AbfsInputStreamHelper getNext() {
    return nextHelper;
  }

  @Override
  public AbfsInputStreamHelper getBack() {
    return null;
  }

  @Override
  public void setNextAsInvalid() {
    isNextHelperValid = false;
  }

  @Override
  public Boolean isAbfsSessionRequired() {
    return false;
  }

  @Override
  public Boolean explicitPreFetchReadAllowed() {
    return true;
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
    return abfsClient.read(path, bytes, sasToken, readRequestParameters,
        tracingContext);
  }
}
