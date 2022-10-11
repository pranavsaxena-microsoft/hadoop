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

package org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamRequestContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsSession;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public interface AbfsInputStreamHelper {

  public boolean shouldGoNext(final AbfsInputStreamContext abfsInputStreamContext);

  public AbfsInputStreamHelper getNext();

  public AbfsInputStreamHelper getBack();

  public void setNextAsInvalid();

  public AbfsRestOperation operate(String path,
      byte[] bytes,
      String sasToken,
      ReadRequestParameters readRequestParameters,
      TracingContext tracingContext,
      AbfsClient abfsClient,
      final AbfsInputStreamRequestContext abfsInputStreamRequestContext)
      throws AzureBlobFileSystemException;

  public Boolean explicitPreFetchReadAllowed();

  public Boolean isAbfsSessionRequired();

  public void setNextAsValid();

  public AbfsSession createAbfsSession(final AbfsClient abfsClient,
      final String path,
      final String eTag,
      final TracingContext tracingContext);
}
