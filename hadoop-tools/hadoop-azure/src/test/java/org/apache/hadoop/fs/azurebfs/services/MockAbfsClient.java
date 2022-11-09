/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

/**
 * Child class of {@link org.apache.hadoop.fs.azurebfs.services.AbfsClient} to orchestrate mocking behaviour for test-classes.
 * It creates an object of {@link org.apache.hadoop.fs.azurebfs.services.MockAbfsRestOperation}
 * and on override call of {@link org.apache.hadoop.fs.azurebfs.services.AbfsClient
 * #getAbfsRestOperation(AbfsRestOperationType, String, URL, List, byte[], int, int, String)}
 * and links it with the test-method provided implementation of
 * {@link org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestIntercept}
 * */
public class MockAbfsClient extends AbfsClient {

  private MockHttpOperationTestIntercept mockHttpOperationTestIntercept;

  public ThreadLocal<Integer> bufferLengthSeen = new ThreadLocal<>();

  public MockAbfsClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final AccessTokenProvider tokenProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, tokenProvider,
        abfsClientContext);
  }

  public MockAbfsClient(final URL baseUrl,
      final SharedKeyCredentials sharedKeyCredentials,
      final AbfsConfiguration abfsConfiguration,
      final SASTokenProvider sasTokenProvider,
      final AbfsClientContext abfsClientContext) throws IOException {
    super(baseUrl, sharedKeyCredentials, abfsConfiguration, sasTokenProvider,
        abfsClientContext);
  }

  public MockAbfsClient(final AbfsClient client) throws IOException {
    super(client.getBaseUrl(), client.getSharedKeyCredentials(),
        client.getAbfsConfiguration(), client.getTokenProvider(),
        client.getAbfsClientContext());
  }

  @Override
  AbfsRestOperation getAbfsRestOperation(final AbfsRestOperationType operationType,
      final String httpMethod,
      final URL url,
      final List<AbfsHttpHeader> requestHeaders,
      final byte[] buffer,
      final int bufferOffset,
      final int bufferLength,
      final String sasTokenForReuse) {
    if (AbfsRestOperationType.ReadFile == operationType) {
      MockAbfsRestOperation op = new MockAbfsRestOperation(
          operationType,
          this,
          httpMethod,
          url,
          requestHeaders,
          buffer,
          bufferOffset,
          bufferLength, sasTokenForReuse);
      op.setMockHttpOperationTestIntercept(mockHttpOperationTestIntercept);
      return op;
    }
    return super.getAbfsRestOperation(operationType, httpMethod, url,
        requestHeaders, buffer, bufferOffset, bufferLength, sasTokenForReuse);
  }

  @Override
  public AbfsRestOperation read(final String path,
      final long position,
      final byte[] buffer,
      final int bufferOffset,
      final int bufferLength,
      final String eTag,
      final String cachedSasToken,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    bufferLengthSeen.set(bufferLength);
    return super.read(path, position, buffer, bufferOffset, bufferLength, eTag,
        cachedSasToken, tracingContext);
  }

  public void setMockHttpOperationTestIntercept(final MockHttpOperationTestIntercept mockHttpOperationTestIntercept) {
    this.mockHttpOperationTestIntercept = mockHttpOperationTestIntercept;
  }
}
