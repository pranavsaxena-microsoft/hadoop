/*
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

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsCounters;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsLease;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsClient;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsOutputStream;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.store.BlockUploadStatistics;
import org.apache.hadoop.fs.store.DataBlocks;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.DATA_BLOCKS_BUFFER;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_BLOCK_UPLOAD_BUFFER_DIR;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.DATA_BLOCKS_BUFFER_DEFAULT;

public class MockAzureBlobFileSystemStore extends AzureBlobFileSystemStore {

  public MockAzureBlobFileSystemStore(final URI uri,
      final boolean isSecureScheme,
      final Configuration configuration,
      final AbfsCounters abfsCounters)
      throws IOException {
    super(new AzureBlobFileSystemStoreBuilder()
        .withAbfsCounters(abfsCounters)
        .withBlockFactory(DataBlocks.createFactory(FS_AZURE_BLOCK_UPLOAD_BUFFER_DIR,
            configuration, configuration.getTrimmed(DATA_BLOCKS_BUFFER,
                DATA_BLOCKS_BUFFER_DEFAULT)))
        .withUri(uri)
        .withConfiguration(configuration)
        .withSecureScheme(isSecureScheme));
  }

  protected MockAbfsInputStream createAbfsInputStreamInstance(final AbfsClient client,
      final FileSystem.Statistics statistics,
      final String path,
      final long contentLength,
      final AbfsInputStreamContext abfsInputStreamContext,
      final String eTag,
      TracingContext tracingContext) {
    try {
      MockAbfsClient mockClient = new MockAbfsClient(client);
      // create instance with fastpath disabled flag. MockAbfsInputStream will setup
      // a mock session valid for 5 mins
      return new MockAbfsInputStream(mockClient, statistics, path, contentLength,
          abfsInputStreamContext.withDefaultOptimizedRest(true), eTag, tracingContext);
    } catch (Exception e) {
      Assert.fail("Failure in creating MockAbfsInputStream " + e.getMessage());
    }

    return null;
  }

  @Override
  public OutputStream openFileForWrite(final Path path,
      final FileSystem.Statistics statistics,
      final boolean overwrite,
      final TracingContext tracingContext) throws IOException {
    return new MockAbfsOutputStream(
        populateAbfsOutputStreamContext(
            false,
            null,
            new MockAbfsClient(getClient()),
            statistics,
            path.getName(),
            0,
            tracingContext));
  }
}
