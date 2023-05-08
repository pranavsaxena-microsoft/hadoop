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

package org.apache.hadoop.fs.azure;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

/**
 * Test atomic renaming.
 */
public class ITestNativeAzureFileSystemAtomicRenameDirList
    extends AbstractWasbTestBase {

  // HBase-site config controlling HBase root dir
  private static final String HBASE_ROOT_DIR_CONF_STRING = "hbase.rootdir";
  private static final String HBASE_ROOT_DIR_VALUE_ON_DIFFERENT_FS =
      "wasb://somedifferentfilesystem.blob.core.windows.net/hbase";

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }

  @Test
  public void testAtomicRenameKeyDoesntNPEOnInitializingWithNonDefaultURI()
      throws IOException {
    NativeAzureFileSystem azureFs = fs;
    AzureNativeFileSystemStore azureStore = azureFs.getStore();
    Configuration conf = fs.getConf();
    conf.set(HBASE_ROOT_DIR_CONF_STRING, HBASE_ROOT_DIR_VALUE_ON_DIFFERENT_FS);
    URI uri = fs.getUri();
    fs.initialize(uri, conf);
    azureStore.isAtomicRenameKey("anyrandomkey");
  }

  @Test
  public void testNewFileCreatedAfterCrash() throws Exception {
    NativeAzureFileSystem azureFs = fs;
    NativeFileSystemStore store = Mockito.spy(fs.getStore());
    azureFs.setStore(store);

    fs.mkdirs(new Path("/hbase/dir1/dir2"));
    fs.create(new Path("/hbase/dir1/dir2/file1"));
    fs.create(new Path("/hbase/dir1/dir2/file2"));
    fs.create(new Path("/hbase/dir1/dir2/file3"));
    fs.mkdirs(new Path("/hbase/dir1/dir2/dir3"));
    fs.create(new Path("/hbase/dir1/dir2/dir3/file"));

    List<SelfRenewingLease> lease = new ArrayList<>();
    Mockito.doAnswer(answer -> {
      SelfRenewingLease leases = (SelfRenewingLease) answer.callRealMethod();
      lease.add(leases);
      return leases;
    }).when(store).acquireLease(Mockito.anyString());

    int[] count = new int[1];
    count[0] = 0;
    Mockito.doAnswer(answer -> {
      count[0]++;
      if(count[0] == 1) {
        throw new IOException("");
      }
      return answer.callRealMethod();
    }).when(store).rename(Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(), Mockito.any(SelfRenewingLease.class));
    try {
      fs.rename(new Path("/hbase/dir1/dir2"), new Path("/hbase/dir3"));
    } catch (Exception e) {

    }
    fs.create(new Path("/hbase/dir1/dir2/dir3/file1"));

    for(SelfRenewingLease lease1 : lease) {
      if(lease1.getLeaseID() != null) {
        try {
          lease1.free();
        } catch (Exception e) {}
      }
    }

    FileStatus[] statuses = fs.listStatus(new Path("/hbase/dir1"));

    Assert.assertTrue(fs.exists(new Path("/hbase/dir1/dir2/dir3/file1")));
    Assert.assertFalse(fs.exists(new Path("/hbase/dir1/dir2/file1")));
  }
}
