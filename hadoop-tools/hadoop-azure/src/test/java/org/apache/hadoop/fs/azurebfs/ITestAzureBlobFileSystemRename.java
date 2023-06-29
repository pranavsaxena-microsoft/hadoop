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

package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobLease;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOpTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsLease;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationTestUtil;
import org.apache.hadoop.fs.azurebfs.services.RenameAtomicityUtils;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LEASE_CREATE_NON_RECURSIVE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_REDIRECT_RENAME;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_INGRESS_FALLBACK_TO_DFS;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ID;
import static org.apache.hadoop.fs.azurebfs.services.RenameAtomicityUtils.SUFFIX;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_ABORTED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_FAILED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_PENDING;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_ABORTED;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_FAILED;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test rename operation.
 */
public class ITestAzureBlobFileSystemRename extends
    AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemRename() throws Exception {
    super();
  }

  @Test
  public void testEnsureFileIsRenamed() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path src = path("testEnsureFileIsRenamed-src");
    touch(src);
    Path dest = path("testEnsureFileIsRenamed-dest");
    fs.delete(dest, true);
    assertRenameOutcome(fs, src, dest, true);

    assertIsFile(fs, dest);
    assertPathDoesNotExist(fs, "expected renamed", src);
  }

  @Test
  public void testRenameWithPreExistingDestination() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path src = path("renameSrc");
    touch(src);
    Path dest = path("renameDest");
    touch(dest);
    assertRenameOutcome(fs, src, dest, false);
  }

  @Test
  public void testRenameFileUnderDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path sourceDir = new Path("/testSrc");
    assertMkdirs(fs, sourceDir);
    String filename = "file1";
    Path file1 = new Path(sourceDir, filename);
    touch(file1);

    Path destDir = new Path("/testDst");
    assertRenameOutcome(fs, sourceDir, destDir, true);
    FileStatus[] fileStatus = fs.listStatus(destDir);
    assertNotNull("Null file status", fileStatus);
    FileStatus status = fileStatus[0];
    assertEquals("Wrong filename in " + status,
        filename, status.getPath().getName());
  }

  @Test
  public void testRenameFileUnderDirRedirection() throws Exception {
    Configuration configuration = Mockito.spy(getRawConfiguration());

    // Set redirect to wasb rename true and assert rename.
    configuration.setBoolean(FS_AZURE_REDIRECT_RENAME, true);
    AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    Path sourceDir = makeQualified(new Path("/testSrc"));
    assertMkdirs(fs1, sourceDir);
    String filename = "file1";
    Path file1 = new Path(sourceDir, filename);
    touch(file1);

    Path destDir = makeQualified(new Path("/testDst"));
    assertRenameOutcome(fs1, sourceDir, destDir, true);
    FileStatus[] fileStatus = fs1.listStatus(destDir);
    assertNotNull("Null file status", fileStatus);
    FileStatus status = fileStatus[0];
    assertEquals("Wrong filename in " + status,
            filename, status.getPath().getName());
  }

  @Test
  public void testRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("testDir"));
    Path test1 = new Path("testDir/test1");
    fs.mkdirs(test1);
    fs.mkdirs(new Path("testDir/test1/test2"));
    fs.mkdirs(new Path("testDir/test1/test2/test3"));

    assertRenameOutcome(fs, test1,
        new Path("testDir/test10"), true);
    assertPathDoesNotExist(fs, "rename source dir", test1);
  }

  @Test
  public void testRenameFirstLevelDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path("/test/" + i);
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          touch(fileName);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    es.shutdownNow();
    Path source = new Path("/test");
    Path dest = new Path("/renamedDir");
    assertRenameOutcome(fs, source, dest, true);

    FileStatus[] files = fs.listStatus(dest);
    assertEquals("Wrong number of files in listing", 1000, files.length);
    assertPathDoesNotExist(fs, "rename source dir", source);
  }

  @Test
  public void testRenameRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assertRenameOutcome(fs,
        new Path("/"),
        new Path("/testRenameRoot"),
        false);
    assertRenameOutcome(fs,
        new Path(fs.getUri().toString() + "/"),
        new Path(fs.getUri().toString() + "/s"),
        false);
  }

  @Test
  public void testPosixRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.mkdirs(new Path("testDir2/test4"));
    Assert.assertTrue(fs.rename(new Path("testDir2/test1/test2/test3"), new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2/test4/test3")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3")));
  }

  @Test
  public void testRenameToRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("/src1/src2"));
    Assert.assertTrue(fs.rename(new Path("/src1/src2"), new Path("/")));
    Assert.assertTrue(fs.exists(new Path("/src2")));
  }

  @Test
  public void testRenameNotFoundBlobToEmptyRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Assert.assertFalse(fs.rename(new Path("/file"), new Path("/")));
  }

  @Test(expected = IOException.class)
  public void testRenameBlobToDstWithColonInPath() throws Exception{
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.create(new Path("/src"));
    fs.rename(new Path("/src"), new Path("/dst:file"));
  }

  @Test
  public void testRenameBlobInSameDirectoryWithNoMarker() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.create(new Path("/srcDir/dir/file"));
    fs.getAbfsStore().getClient().deleteBlobPath(new Path("/srcDir/dir"), null,
        Mockito.mock(TracingContext.class));
    Assert.assertTrue(fs.rename(new Path("/srcDir/dir"), new Path("/srcDir")));
  }

  /**
   * <pre>
   * Test to check behaviour of rename API if the destination directory is already
   * there. The HNS call and the one for Blob endpoint should have same behaviour.
   *
   * /testDir2/test1/test2/test3 contains (/file)
   * There is another path that exists: /testDir2/test4/test3
   * On rename(/testDir2/test1/test2/test3, /testDir2/test4).
   * </pre>
   *
   * Expectation for HNS / Blob endpoint:<ol>
   * <li>Rename should fail</li>
   * <li>No file should be transferred to destination directory</li>
   * </ol>
   */
  @Test
  public void testPosixRenameDirectoryWhereDirectoryAlreadyThereOnDestination()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.create(new Path("testDir2/test1/test2/test3/file"));
    fs.mkdirs(new Path("testDir2/test4/test3"));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    Assert.assertFalse(fs.rename(new Path("testDir2/test1/test2/test3"),
        new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3")));
    if (getIsNamespaceEnabled(fs)
        || fs.getAbfsStore().getAbfsConfiguration().getPrefixMode()
        == PrefixMode.BLOB) {
      assertFalse(fs.exists(new Path("testDir2/test4/test3/file")));
      assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    } else {
      assertTrue(fs.exists(new Path("testDir2/test4/test3/file")));
      assertFalse(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    }
  }

  @Test
  public void testPosixRenameDirectoryWherePartAlreadyThereOnDestination()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.create(new Path("testDir2/test1/test2/test3/file"));
    fs.create(new Path("testDir2/test1/test2/test3/file1"));
    fs.mkdirs(new Path("testDir2/test4/"));
    fs.create(new Path("testDir2/test4/file1"));
    byte[] etag = fs.getXAttr(new Path("testDir2/test4/file1"), "ETag");
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file1")));
    Assert.assertTrue(fs.rename(new Path("testDir2/test1/test2/test3"),
        new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3")));


    assertFalse(fs.exists(new Path("testDir2/test4/file")));
    assertTrue(fs.exists(new Path("testDir2/test4/file1")));
    assertTrue(fs.exists(new Path("testDir2/test4/test3/file")));
    assertTrue(fs.exists(new Path("testDir2/test4/test3/file1")));
    assertTrue(fs.exists(new Path("testDir2/test4/file1")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3/file1")));
  }

  private void assumeNonHnsAccountBlobEndpoint(final AzureBlobFileSystem fs) {
    Assume.assumeTrue("To work on only on non-HNS Blob endpoint",
        fs.getAbfsStore().getAbfsConfiguration().getPrefixMode()
            == PrefixMode.BLOB);
  }

  /**
   * Test that after completing rename for a directory which is enabled for
   * AtomicRename, the RenamePending JSON file is deleted.
   */
  @Test
  public void testRenamePendingJsonIsRemovedPostSuccessfulRename()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path("hbase/test1/test2/test3/file1"));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    Mockito.doAnswer(answer -> {
      final String correctDeletePath = "/hbase/test1/test2/test3" + SUFFIX;
      if (correctDeletePath.equals(
          ((Path) answer.getArgument(0)).toUri().getPath())) {
        correctDeletePathCount[0] = 1;
      }
      return null;
    }).when(spiedFs).delete(Mockito.any(Path.class), Mockito.anyBoolean());
    Assert.assertTrue(spiedFs.rename(new Path("hbase/test1/test2/test3"),
        new Path("hbase/test4")));
    Assert.assertTrue(correctDeletePathCount[0] == 1);
  }

  /**
   * Test for a directory in /hbase directory. To simulate the crash of process,
   * test will throw an exception with 503 on a copy of one of the blob.<br>
   * ListStatus API will be called on the directory. Expectation is that the ListStatus
   * API of {@link AzureBlobFileSystem} should recover the paused rename.
   */
  @Test
  public void testHBaseHandlingForFailedRename() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    final String failedCopyPath = "hbase/test1/test2/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedAbfsStore = Mockito.spy(
        spiedFs.getAbfsStore());
    Mockito.doReturn(spiedAbfsStore).when(spiedFs).getAbfsStore();
    AbfsClient spiedClient = Mockito.spy(spiedAbfsStore.getClient());
    spiedAbfsStore.setClient(spiedClient);
    Map<String, String> pathLeaseIdMap = new HashMap<>();
    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = (AbfsRestOperation) answer.callRealMethod();
      String leaseId = op.getResult().getResponseHeader(X_MS_LEASE_ID);
      pathLeaseIdMap.put(answer.getArgument(0), leaseId);
      return op;
    }).when(spiedClient).acquireBlobLease(Mockito.anyString(), Mockito.anyInt(), Mockito.any(TracingContext.class));
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    //fail copy of /hbase/test1/test2/test3/file1.
    Mockito.doAnswer(answer -> {
          final Path srcPath = answer.getArgument(0);
          final Path dstPath = answer.getArgument(1);
          final String leaseId = answer.getArgument(2);
          final TracingContext tracingContext = answer.getArgument(3);
          if (("/" + failedCopyPath).equalsIgnoreCase(srcPath.toUri().getPath())) {
            throw new AbfsRestOperationException(HttpURLConnection.HTTP_UNAVAILABLE,
                AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT.getErrorCode(),
                "Ingress is over the account limit.", new Exception());
          }
          fs.getAbfsStore().copyBlob(srcPath, dstPath, leaseId, tracingContext);
          return null;
        })
        .when(spiedAbfsStore)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    try {
      spiedFs.rename(new Path("hbase/test1/test2/test3"),
          new Path("hbase/test4"));
    } catch (Exception ex) {

    }
    Assert.assertTrue(fs.exists(new Path(failedCopyPath)));
    Assert.assertFalse(spiedFs.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test3/"))));

    //call listPath API, it will recover the rename atomicity.
    final AzureBlobFileSystem spiedFsForListPath = Mockito.spy(fs);
    final int[] openRequiredFile = new int[1];
    openRequiredFile[0] = 0;
    Mockito.doAnswer(answer -> {
      final Path path = answer.getArgument(0);
      if (("/" + "hbase/test1/test2/test3" + SUFFIX).equalsIgnoreCase(
          path.toUri().getPath())) {
        openRequiredFile[0] = 1;
      }
      return fs.open(path);
    }).when(spiedFsForListPath).open(Mockito.any(Path.class));

    /*
     * Check if the fs.delete is on the renameJson file.
     */
    AtomicInteger deletedCount = new AtomicInteger(0);
    AtomicBoolean srcSuffixDeletion = new AtomicBoolean(false);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          Boolean recursive = answer.getArgument(1);
          Assert.assertTrue(
              ("/" + "hbase/test1/test2/test3" + SUFFIX).equalsIgnoreCase(
                  path.toUri().getPath()));
          srcSuffixDeletion.set(true);
          deletedCount.incrementAndGet();
          return fs.delete(path, recursive);
        })
        .when(spiedFsForListPath)
        .delete(Mockito.any(Path.class), Mockito.anyBoolean());

    /*
     * Check if the blob which will be retried is deleted from the renameBlob
     * method.
     */
    AbfsClient client = spiedFsForListPath.getAbfsClient();
    final AbfsClient spiedClientForListPath = Mockito.spy(client);
    spiedFsForListPath.getAbfsStore().setClient(spiedClientForListPath);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          String leaseId = answer.getArgument(1);
          TracingContext tracingContext = answer.getArgument(2);
          if (srcSuffixDeletion.get()) {
            Assert.assertTrue(
                ("/" + "hbase/test1/test2/test3" + SUFFIX).equalsIgnoreCase(
                    path.toUri().getPath()));
          } else {
            Assert.assertTrue(
                ("/" + failedCopyPath).equalsIgnoreCase(path.toUri().getPath())
                    || "/hbase/test1/test2/test3".equalsIgnoreCase(
                    path.toUri().getPath()));
            deletedCount.incrementAndGet();
          }
          client.deleteBlobPath(path, leaseId, tracingContext);
          return null;
        })
        .when(spiedClientForListPath)
        .deleteBlobPath(Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));

    for(Map.Entry<String, String> entry : pathLeaseIdMap.entrySet()) {
      try {
        fs.getAbfsClient()
            .releaseBlobLease(entry.getKey(), entry.getValue(),
                Mockito.mock(TracingContext.class));
      } catch (AbfsRestOperationException ex) {

      }
    }
    spiedFsForListPath.listStatus(new Path("hbase/test1/test2"));
    Assert.assertTrue(openRequiredFile[0] == 1);
    Assert.assertTrue(deletedCount.get() == 3);
    Assert.assertFalse(spiedFsForListPath.exists(new Path(failedCopyPath)));
    Assert.assertTrue(spiedFsForListPath.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test3/"))));
  }

  /**
   * Test for a directory in /hbase directory. To simulate the crash of process,
   * test will throw an exception with 503 on a copy of one of the blob. The
   * source directory is a nested directory.<br>
   * ListStatus API will be called on the directory. Expectation is that the ListStatus
   * API of {@link AzureBlobFileSystem} should recover the paused rename.
   */
  @Test
  public void testHBaseHandlingForFailedRenameForNestedSourceThroughListFile()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    final String failedCopyPath = "hbase/test1/test2/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedAbfsStore = Mockito.spy(
        spiedFs.getAbfsStore());
    Mockito.doReturn(spiedAbfsStore).when(spiedFs).getAbfsStore();
    AbfsClient spiedClient = Mockito.spy(spiedAbfsStore.getClient());
    spiedAbfsStore.setClient(spiedClient);
    Map<String, String> pathLeaseIdMap = new HashMap<>();
    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = (AbfsRestOperation) answer.callRealMethod();
      String leaseId = op.getResult().getResponseHeader(X_MS_LEASE_ID);
      pathLeaseIdMap.put(answer.getArgument(0), leaseId);
      return op;
    }).when(spiedClient).acquireBlobLease(Mockito.anyString(), Mockito.anyInt(), Mockito.any(TracingContext.class));
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    //fail copy of /hbase/test1/test2/test3/file1.
    Mockito.doAnswer(answer -> {
          final Path srcPath = answer.getArgument(0);
          final Path dstPath = answer.getArgument(1);
          final String leaseId = answer.getArgument(2);
          final TracingContext tracingContext = answer.getArgument(3);
          if (("/" + failedCopyPath).equalsIgnoreCase(srcPath.toUri().getPath())) {
            throw new AbfsRestOperationException(HttpURLConnection.HTTP_UNAVAILABLE,
                AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT.getErrorCode(),
                "Ingress is over the account limit.", new Exception());
          }
          fs.getAbfsStore().copyBlob(srcPath, dstPath, leaseId, tracingContext);
          return null;
        })
        .when(spiedAbfsStore)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    try {
      spiedFs.rename(new Path("hbase/test1/test2"),
          new Path("hbase/test4"));
    } catch (Exception ex) {

    }
    Assert.assertTrue(fs.exists(new Path(failedCopyPath)));
    Assert.assertFalse(spiedFs.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test3/"))));

    //call listPath API, it will recover the rename atomicity.
    final AzureBlobFileSystem spiedFsForListPath = Mockito.spy(fs);
    final int[] openRequiredFile = new int[1];
    openRequiredFile[0] = 0;
    Mockito.doAnswer(answer -> {
      final Path path = answer.getArgument(0);
      if (("/" + "hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
          path.toUri().getPath())) {
        openRequiredFile[0] = 1;
      }
      return fs.open(path);
    }).when(spiedFsForListPath).open(Mockito.any(Path.class));

    /*
     * Check if the fs.delete is on the renameJson file.
     */
    AtomicInteger deletedCount = new AtomicInteger(0);
    AtomicBoolean srcDirSuffixDeletion = new AtomicBoolean(false);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          Boolean recursive = answer.getArgument(1);
          Assert.assertTrue(("/" + "hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
              path.toUri().getPath()));
          srcDirSuffixDeletion.set(true);
          deletedCount.incrementAndGet();
          return fs.delete(path, recursive);
        })
        .when(spiedFsForListPath)
        .delete(Mockito.any(Path.class), Mockito.anyBoolean());

    /*
     * Check if the blob which will be retried is deleted from the renameBlob
     * method.
     */
    AbfsClient client = spiedFsForListPath.getAbfsClient();
    final AbfsClient spiedClientForListPath = Mockito.spy(client);
    spiedFsForListPath.getAbfsStore().setClient(spiedClientForListPath);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          String leaseId = answer.getArgument(1);
          TracingContext tracingContext = answer.getArgument(2);
          if (srcDirSuffixDeletion.get()) {
            Assert.assertTrue(("/" + "hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
                path.toUri().getPath()));
          } else {
            Assert.assertTrue(
                ("/" + failedCopyPath).equalsIgnoreCase(path.toUri().getPath())
                    || "/hbase/test1/test2".equalsIgnoreCase(
                    path.toUri().getPath()));
            deletedCount.incrementAndGet();
          }
          client.deleteBlobPath(path, leaseId, tracingContext);
          return null;
        })
        .when(spiedClientForListPath)
        .deleteBlobPath(Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));

    /*
     * listFile on /hbase/test1 would give no result because
     * /hbase/test1/test2 would be totally moved to /hbase/test4.
     *
     */
    for(Map.Entry<String, String> entry : pathLeaseIdMap.entrySet()) {
      try {
        fs.getAbfsClient()
            .releaseBlobLease(entry.getKey(), entry.getValue(),
                Mockito.mock(TracingContext.class));
      } catch (AbfsRestOperationException ex) {

      }
    }
    final FileStatus[] listFileResult = spiedFsForListPath.listStatus(
        new Path("hbase/test1"));
    Assert.assertTrue(openRequiredFile[0] == 1);
    Assert.assertTrue(deletedCount.get() == 3);
    Assert.assertFalse(spiedFsForListPath.exists(new Path(failedCopyPath)));
    Assert.assertTrue(spiedFsForListPath.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test2/test3/"))));
    Assert.assertTrue(listFileResult.length == 0);
  }

  /**
   * Test for a directory in /hbase directory. To simulate the crash of process,
   * test will throw an exception with 503 on a copy of one of the blob. The
   * source directory is a nested directory.<br>
   * GetFileStatus API will be called on the directory. Expectation is that the
   * GetFileStatus API of {@link AzureBlobFileSystem} should recover the paused
   * rename.
   */
  @Test
  public void testHBaseHandlingForFailedRenameForNestedSourceThroughGetPathStatus()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    final String failedCopyPath = "hbase/test1/test2/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedAbfsStore = Mockito.spy(
        spiedFs.getAbfsStore());
    Mockito.doReturn(spiedAbfsStore).when(spiedFs).getAbfsStore();
    AbfsClient spiedClient = Mockito.spy(spiedAbfsStore.getClient());
    spiedAbfsStore.setClient(spiedClient);
    Map<String, String> pathLeaseIdMap = new HashMap<>();
    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = (AbfsRestOperation) answer.callRealMethod();
      String leaseId = op.getResult().getResponseHeader(X_MS_LEASE_ID);
      pathLeaseIdMap.put(answer.getArgument(0), leaseId);
      return op;
    }).when(spiedClient).acquireBlobLease(Mockito.anyString(), Mockito.anyInt(), Mockito.any(TracingContext.class));
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    //fail copy of /hbase/test1/test2/test3/file1.
    Mockito.doAnswer(answer -> {
          final Path srcPath = answer.getArgument(0);
          final Path dstPath = answer.getArgument(1);
          final String leaseId = answer.getArgument(2);
          final TracingContext tracingContext = answer.getArgument(3);
          if (("/" + failedCopyPath).equalsIgnoreCase(srcPath.toUri().getPath())) {
            throw new AbfsRestOperationException(HttpURLConnection.HTTP_UNAVAILABLE,
                AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT.getErrorCode(),
                "Ingress is over the account limit.", new Exception());
          }
          fs.getAbfsStore().copyBlob(srcPath, dstPath, leaseId, tracingContext);
          return null;
        })
        .when(spiedAbfsStore)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    try {
      spiedFs.rename(new Path("hbase/test1/test2"),
          new Path("hbase/test4"));
    } catch (Exception ex) {

    }
    Assert.assertTrue(fs.exists(new Path(failedCopyPath)));
    Assert.assertFalse(spiedFs.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test3/"))));

    //call listPath API, it will recover the rename atomicity.
    final AzureBlobFileSystem spiedFsForListPath = Mockito.spy(fs);
    final int[] openRequiredFile = new int[1];
    openRequiredFile[0] = 0;
    Mockito.doAnswer(answer -> {
      final Path path = answer.getArgument(0);
      if (("/" + "hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
          path.toUri().getPath())) {
        openRequiredFile[0] = 1;
      }
      return fs.open(path);
    }).when(spiedFsForListPath).open(Mockito.any(Path.class));

    /*
     * Check if the fs.delete is on the renameJson file.
     */
    AtomicInteger deletedCount = new AtomicInteger(0);
    AtomicBoolean srcSuffixDeletion = new AtomicBoolean(false);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          Boolean recursive = answer.getArgument(1);
          Assert.assertTrue(("/" + "hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
              path.toUri().getPath()));
          srcSuffixDeletion.set(true);
          deletedCount.incrementAndGet();
          return fs.delete(path, recursive);
        })
        .when(spiedFsForListPath)
        .delete(Mockito.any(Path.class), Mockito.anyBoolean());

    /*
     * Check if the blob which will be retried is deleted from the renameBlob
     * method.
     */
    AbfsClient client = spiedFsForListPath.getAbfsClient();
    final AbfsClient spiedClientForListPath = Mockito.spy(client);
    spiedFsForListPath.getAbfsStore().setClient(spiedClientForListPath);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          String leaseId = answer.getArgument(1);
          TracingContext tracingContext = answer.getArgument(2);
          if (srcSuffixDeletion.get()) {
            Assert.assertTrue(("/" + "hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
                path.toUri().getPath()));
          } else {
            Assert.assertTrue(
                ("/" + failedCopyPath).equalsIgnoreCase(path.toUri().getPath())
                    || "/hbase/test1/test2".equalsIgnoreCase(
                    path.toUri().getPath()));
            deletedCount.incrementAndGet();
          }
          client.deleteBlobPath(path, leaseId, tracingContext);
          return null;
        })
        .when(spiedClientForListPath)
        .deleteBlobPath(Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));

    /*
     * getFileStatus on /hbase/test2 should give NOT_FOUND exception, since,
     * /hbase/test1/test2 was partially renamed. On the invocation of getFileStatus
     * on the directory, the remaining rename will be made. And as the directory is renamed,
     * the method should give NOT_FOUND exception.
     */
    for(Map.Entry<String, String> entry : pathLeaseIdMap.entrySet()) {
      try {
        fs.getAbfsClient().releaseBlobLease(entry.getKey(), entry.getValue(), Mockito.mock(TracingContext.class));
      } catch (AbfsRestOperationException ex) {

      }
    }
    FileStatus fileStatus = null;
    Boolean notFoundExceptionReceived = false;
    try {
      fileStatus = spiedFsForListPath.getFileStatus(
          new Path("hbase/test1/test2"));
    } catch (FileNotFoundException ex) {
      notFoundExceptionReceived = true;

    }
    Assert.assertTrue(notFoundExceptionReceived);
    Assert.assertNull(fileStatus);
    Assert.assertTrue(openRequiredFile[0] == 1);
    Assert.assertTrue(deletedCount.get() == 3);
    Assert.assertFalse(spiedFsForListPath.exists(new Path(failedCopyPath)));
    Assert.assertTrue(spiedFsForListPath.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test2/test3/"))));
  }

  /**
   * Simulates a scenario where HMaster in Hbase starts up and executes listStatus
   * API on the directory that has to be renamed by some other executor-machine.
   * The scenario is that RenamePending JSON is created but before it could be
   * appended, it has been opened by the HMaster. The HMaster will delete it. The
   * machine doing rename would have to recreate the JSON file.
   * ref: <a href="https://issues.apache.org/jira/browse/HADOOP-12678">issue</a>
   */
  @Test
  public void testHbaseListStatusBeforeRenamePendingFileAppendedWithIngressOnBlob()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path("/"));
    testHbaseListStatusBeforeRenamePendingFileAppended(fs);
  }

  @Test
  public void testHbaseListStatusBeforeRenamePendingFileAppendedWithIngressOnDFS()
      throws Exception {
    AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);


    Configuration configuration = Mockito.spy(fs.getAbfsStore().getAbfsConfiguration().getRawConfiguration());
    configuration.set(FS_AZURE_INGRESS_FALLBACK_TO_DFS, TRUE);
    fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    fs.setWorkingDirectory(new Path("/"));
    testHbaseListStatusBeforeRenamePendingFileAppended(fs);
  }

  private void testHbaseListStatusBeforeRenamePendingFileAppended(final AzureBlobFileSystem fs) throws IOException {
    final String failedCopyPath = "hbase/test1/test2/test3/file1";
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedAbfsStore = Mockito.spy(
        spiedFs.getAbfsStore());
    Mockito.doReturn(spiedAbfsStore).when(spiedFs).getAbfsStore();
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    Boolean[] renamePendingJsonCreated = new Boolean[1];
    renamePendingJsonCreated[0] = false;
    Boolean[] parallelListStatusCalledOnTheDirBeingRenamed = new Boolean[1];
    parallelListStatusCalledOnTheDirBeingRenamed[0] = false;
    Mockito.doAnswer(answer -> {
      Path path = answer.getArgument(0);
      Boolean recursive = answer.getArgument(1);
      FSDataOutputStream outputStream = fs.create(path, recursive);
      renamePendingJsonCreated[0] = true;
      while (!parallelListStatusCalledOnTheDirBeingRenamed[0]) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return outputStream;
    }).when(spiedFs).create(Mockito.any(Path.class), Mockito.anyBoolean());

    try {
      new Thread(() -> {
        //wait for the renamePending created;
        while (!renamePendingJsonCreated[0]) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        try {
          spiedFs.listStatus(new Path("hbase/test1"));
          parallelListStatusCalledOnTheDirBeingRenamed[0] = true;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).start();
      spiedFs.rename(new Path("hbase/test1/test2"),
          new Path("hbase/test4"));
    } catch (Exception ex) {

    }
    Assert.assertFalse(fs.exists(new Path(failedCopyPath)));
    Assert.assertTrue(
        spiedFs.exists(new Path(failedCopyPath.replace("test1/", "test4/"))));
  }

  @Test
  public void testHbaseEmptyRenamePendingJsonDeletedBeforeListStatusCanDelete()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    final String failedCopyPath = "hbase/test1/test2/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedStore = Mockito.spy(
        spiedFs.getAbfsStore());
    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    AzureBlobFileSystem listFileFs = Mockito.spy(fs);


    Boolean[] renamePendingJsonCreated = new Boolean[1];
    renamePendingJsonCreated[0] = false;
    Boolean[] parallelListStatusCalledOnTheDirBeingRenamed = new Boolean[1];
    parallelListStatusCalledOnTheDirBeingRenamed[0] = false;
    Boolean[] parallelDeleteOfRenamePendingFileFromRenameFlow = new Boolean[1];
    parallelDeleteOfRenamePendingFileFromRenameFlow[0] = false;
    Mockito.doAnswer(answer -> {
      Path path = answer.getArgument(0);
      Boolean recursive = answer.getArgument(1);
      FSDataOutputStream outputStream = fs.create(path, recursive);
      renamePendingJsonCreated[0] = true;
      while (!parallelListStatusCalledOnTheDirBeingRenamed[0]) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return outputStream;
    }).when(spiedFs).create(Mockito.any(Path.class), Mockito.anyBoolean());

    Mockito.doAnswer(answer -> {
      Path path = answer.getArgument(0);
      Boolean recursive = answer.getArgument(1);
      if (("/hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
          path.toUri().getPath())) {
        while (!parallelListStatusCalledOnTheDirBeingRenamed[0]) {
          Thread.sleep(100);
        }
        parallelDeleteOfRenamePendingFileFromRenameFlow[0] = true;
        return fs.delete(path, recursive);
      }
      return fs.delete(path, recursive);
    }).when(spiedFs).delete(Mockito.any(Path.class), Mockito.anyBoolean());

    Mockito.doAnswer(answer -> {
      Path path = answer.getArgument(0);
      if (("/hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
          path.toUri().getPath())) {
        FSDataInputStream inputStream = fs.open(path);
        parallelListStatusCalledOnTheDirBeingRenamed[0] = true;
        while (!parallelDeleteOfRenamePendingFileFromRenameFlow[0]) {
          Thread.sleep(100);
        }
        return inputStream;
      }
      return fs.open(path);
    }).when(listFileFs).open(Mockito.any(Path.class));

    try {
      new Thread(() -> {
        //wait for the renamePending created;
        while (!renamePendingJsonCreated[0]) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        try {
          listFileFs.listStatus(new Path("hbase/test1"));
          parallelListStatusCalledOnTheDirBeingRenamed[0] = true;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).start();
      spiedFs.rename(new Path("hbase/test1/test2"),
          new Path("hbase/test4"));
    } catch (Exception ex) {

    }
    Assert.assertFalse(fs.exists(new Path(failedCopyPath)));
    Assert.assertTrue(
        spiedFs.exists(new Path(failedCopyPath.replace("test1/", "test4/"))));
  }

  @Test
  public void testInvalidJsonForRenamePendingFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path("hbase/test1/test2/test3/file1"));
    FSDataOutputStream outputStream = fs.create(
        new Path("hbase/test1/test2/test3" + SUFFIX));
    outputStream.writeChars("{ some wrong json");
    outputStream.flush();
    outputStream.close();

    fs.listStatus(new Path("hbase/test1/test2"));
    Assert.assertFalse(fs.exists(new Path("hbase/test1/test2/test3" + SUFFIX)));
  }

  @Test
  public void testEmptyDirRenameResolveFromListStatus() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    String srcDir = "/hbase/test1/test2/test3";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path(srcDir));
    fs.create(new Path(srcDir, "file1"));
    fs.mkdirs(new Path("hbase/test4"));

    AzureBlobFileSystem spiedFs = Mockito.spy(fs);

    AzureBlobFileSystemStore spiedAbfsStore = Mockito.spy(
        spiedFs.getAbfsStore());
    Mockito.doReturn(spiedAbfsStore).when(spiedFs).getAbfsStore();
    AbfsClient spiedClient = Mockito.spy(spiedAbfsStore.getClient());
    spiedAbfsStore.setClient(spiedClient);
    Map<String, String> pathLeaseIdMap = new HashMap<>();
    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = (AbfsRestOperation) answer.callRealMethod();
      String leaseId = op.getResult().getResponseHeader(X_MS_LEASE_ID);
      pathLeaseIdMap.put(answer.getArgument(0), leaseId);
      return op;
    }).when(spiedClient).acquireBlobLease(Mockito.anyString(), Mockito.anyInt(), Mockito.any(TracingContext.class));
    Mockito.doAnswer(answer -> {
          throw new AbfsRestOperationException(HttpURLConnection.HTTP_UNAVAILABLE,
              AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT.getErrorCode(),
              "Ingress is over the account limit.", new Exception());
        })
        .when(spiedAbfsStore)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    try {
      spiedFs.rename(new Path(srcDir),
          new Path("hbase/test4"));
    } catch (Exception ex) {
    }

    Assert.assertFalse(spiedFs.exists(
        new Path(srcDir.replace("test1/test2/test3", "test4/test3/"))));

    //call listPath API, it will recover the rename atomicity.
    for(Map.Entry<String, String> entry : pathLeaseIdMap.entrySet()) {
      try {
        fs.getAbfsClient()
            .releaseBlobLease(entry.getKey(), entry.getValue(),
                Mockito.mock(TracingContext.class));
      } catch (Exception e) {}
    }

    final AzureBlobFileSystem spiedFsForListPath = Mockito.spy(fs);
    final int[] openRequiredFile = new int[1];
    openRequiredFile[0] = 0;
    Mockito.doAnswer(answer -> {
      final Path path = answer.getArgument(0);
      if ((srcDir + SUFFIX).equalsIgnoreCase(path.toUri().getPath())) {
        openRequiredFile[0] = 1;
      }
      return fs.open(path);
    }).when(spiedFsForListPath).open(Mockito.any(Path.class));

    /*
     * Check if the fs.delete is on the renameJson file.
     */
    AtomicInteger deletedCount = new AtomicInteger(0);
    AtomicBoolean deletedSrcDirSuffix = new AtomicBoolean(false);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          Boolean recursive = answer.getArgument(1);
          Assert.assertTrue(
              (srcDir + SUFFIX).equalsIgnoreCase(path.toUri().getPath()));
          deletedSrcDirSuffix.set(true);
          deletedCount.incrementAndGet();
          return fs.delete(path, recursive);
        })
        .when(spiedFsForListPath)
        .delete(Mockito.any(Path.class), Mockito.anyBoolean());

    /*
     * Check if the blob which will be retried is deleted from the renameBlob
     * method.
     */
    AbfsClient client = spiedFsForListPath.getAbfsClient();
    final AbfsClient spiedClientForListPath = Mockito.spy(client);
    spiedFsForListPath.getAbfsStore().setClient(spiedClientForListPath);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          String leaseId = answer.getArgument(1);
          TracingContext tracingContext = answer.getArgument(2);
          if (deletedSrcDirSuffix.get()) {
            Assert.assertTrue(
                (srcDir + SUFFIX).equalsIgnoreCase(path.toUri().getPath()));
          } else {
            Assert.assertTrue(
                ((srcDir).equalsIgnoreCase(path.toUri().getPath()) || (srcDir
                    + "/file1").equalsIgnoreCase(path.toUri().getPath())));
            deletedCount.incrementAndGet();
          }
          client.deleteBlobPath(path, leaseId, tracingContext);
          return null;
        })
        .when(spiedClientForListPath)
        .deleteBlobPath(Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));

    /*
     * getFileStatus on /hbase/test2 should give NOT_FOUND exception, since,
     * /hbase/test1/test2 was partially renamed. On the invocation of getFileStatus
     * on the directory, the remaining rename will be made. And as the directory is renamed,
     * the method should give NOT_FOUND exception.
     */
    FileStatus fileStatus = null;
    Boolean notFoundExceptionReceived = false;
    try {
      fileStatus = spiedFsForListPath.getFileStatus(new Path(srcDir));
    } catch (FileNotFoundException ex) {
      notFoundExceptionReceived = true;

    }
    Assert.assertTrue(notFoundExceptionReceived);
    Assert.assertNull(fileStatus);
    Assert.assertTrue(openRequiredFile[0] == 1);
    Assert.assertTrue(deletedCount.get() == 3);
    Assert.assertFalse(spiedFsForListPath.exists(new Path(srcDir)));
    Assert.assertTrue(spiedFsForListPath.getFileStatus(
            new Path(srcDir.replace("test1/test2/test3", "test4/test3/")))
        .isDirectory());
  }

  @Test
  public void testRenameBlobIdempotency() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    String srcDir = "/test1/test2/test3";
    fs.mkdirs(new Path(srcDir));
    fs.create(new Path(srcDir, "file1"));
    fs.create(new Path(srcDir, "file2"));

    fs.mkdirs(new Path("/test4"));

    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedStore = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    final AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());
    spiedStore.setClient(spiedClient);

    /*
     * First call to copyBlob for file1 will fail with connection-reset, but the
     * backend has got the call. Retry of that API would give 409 error.
     */
    boolean[] hasBeenCalled = new boolean[1];
    hasBeenCalled[0] = false;

    boolean[] connectionResetThrown = new boolean[1];
    connectionResetThrown[0] = false;

    AbfsClientTestUtil.setMockAbfsRestOperationForCopyBlobOperation(spiedClient,
        (spiedRestOp, actualCallMakerOp) -> {

          Mockito.doAnswer(answer -> {
            if (spiedRestOp.getUrl().toString().contains("file1")
                && !hasBeenCalled[0]) {
              hasBeenCalled[0] = true;
              actualCallMakerOp.execute(answer.getArgument(0));
              AbfsRestOperationTestUtil.addAbfsHttpOpProcessResponseMock(
                  spiedRestOp, (mockAbfsHttpOp, actualAbfsHttpOp) -> {
                    Mockito.doAnswer(sendRequestAnswer -> {
                          if (!connectionResetThrown[0]) {
                            connectionResetThrown[0] = true;
                            throw new SocketException("connection-reset");
                          }
                          spiedRestOp.signRequest(actualAbfsHttpOp,
                              sendRequestAnswer.getArgument(2));
                          actualAbfsHttpOp.sendRequest(
                              sendRequestAnswer.getArgument(0),
                              sendRequestAnswer.getArgument(1),
                              sendRequestAnswer.getArgument(2));
                          AbfsHttpOpTestUtil.setConnection(mockAbfsHttpOp,
                              actualAbfsHttpOp);
                          return mockAbfsHttpOp;
                        }).when(mockAbfsHttpOp)
                        .sendRequest(Mockito.nullable(byte[].class),
                            Mockito.anyInt(), Mockito.anyInt());

                    return mockAbfsHttpOp;
                  });
              Mockito.doCallRealMethod()
                  .when(spiedRestOp)
                  .execute(Mockito.any(TracingContext.class));
              spiedRestOp.execute(answer.getArgument(0));
              return spiedRestOp;
            } else {
              actualCallMakerOp.execute(answer.getArgument(0));
              AbfsRestOperationTestUtil.setResult(spiedRestOp,
                  actualCallMakerOp.getResult());
              return actualCallMakerOp;
            }
          }).when(spiedRestOp).execute(Mockito.any(TracingContext.class));
          return spiedRestOp;
        });

    spiedFs.setWorkingDirectory(new Path("/"));

    Assert.assertTrue(spiedFs.rename(new Path(srcDir), new Path("/test4")));
    Assert.assertTrue(spiedFs.exists(new Path("test4/test3/file1")));
    Assert.assertTrue(spiedFs.exists(new Path("test4/test3/file2")));
    Assert.assertTrue(hasBeenCalled[0]);
    Assert.assertTrue(connectionResetThrown[0]);
  }

  @Test
  public void testRenameBlobIdempotencyWhereDstIsCreatedFromSomeOtherProcess()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    String srcDir = "/test1/test2/test3";
    fs.mkdirs(new Path(srcDir));
    fs.create(new Path(srcDir, "file1"));
    fs.create(new Path(srcDir, "file2"));

    fs.mkdirs(new Path("/test4"));

    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedStore = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    final AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());
    spiedStore.setClient(spiedClient);

    /*
     * First call to copyBlob for file1 will fail with connection-reset, but the
     * backend has got the call. Retry of that API would give 409 error.
     */
    boolean[] hasBeenCalled = new boolean[1];
    hasBeenCalled[0] = false;

    boolean[] connectionResetThrown = new boolean[1];
    connectionResetThrown[0] = false;

    AbfsClientTestUtil.setMockAbfsRestOperationForCopyBlobOperation(spiedClient,
        (spiedRestOp, actualCallMakerOp) -> {

          Mockito.doAnswer(answer -> {
            if (spiedRestOp.getUrl().toString().contains("file1")
                && !hasBeenCalled[0]) {
              hasBeenCalled[0] = true;
              fs.create(new Path("/test4/test3", "file1"));
              AbfsRestOperationTestUtil.addAbfsHttpOpProcessResponseMock(
                  spiedRestOp, (mockAbfsHttpOp, actualAbfsHttpOp) -> {
                    Mockito.doAnswer(sendRequestAnswer -> {
                          if (!connectionResetThrown[0]) {
                            connectionResetThrown[0] = true;
                            throw new SocketException("connection-reset");
                          }
                          spiedRestOp.signRequest(actualAbfsHttpOp,
                              sendRequestAnswer.getArgument(2));
                          actualAbfsHttpOp.sendRequest(
                              sendRequestAnswer.getArgument(0),
                              sendRequestAnswer.getArgument(1),
                              sendRequestAnswer.getArgument(2));
                          AbfsHttpOpTestUtil.setConnection(mockAbfsHttpOp,
                              actualAbfsHttpOp);
                          return mockAbfsHttpOp;
                        }).when(mockAbfsHttpOp)
                        .sendRequest(Mockito.nullable(byte[].class),
                            Mockito.anyInt(), Mockito.anyInt());

                    return mockAbfsHttpOp;
                  });
              Mockito.doCallRealMethod()
                  .when(spiedRestOp)
                  .execute(Mockito.any(TracingContext.class));
              spiedRestOp.execute(answer.getArgument(0));
              return spiedRestOp;
            } else {
              actualCallMakerOp.execute(answer.getArgument(0));
              AbfsRestOperationTestUtil.setResult(spiedRestOp,
                  actualCallMakerOp.getResult());
              return actualCallMakerOp;
            }
          }).when(spiedRestOp).execute(Mockito.any(TracingContext.class));
          return spiedRestOp;
        });

    spiedFs.setWorkingDirectory(new Path("/"));

    Boolean dstAlreadyThere = false;
    try {
      spiedFs.rename(new Path(srcDir), new Path("/test4"));
    } catch (RuntimeException ex) {
      if (ex.getMessage().contains(HttpURLConnection.HTTP_CONFLICT + "")) {
        dstAlreadyThere = true;
      }
    }
    Assert.assertTrue(dstAlreadyThere);
    Assert.assertTrue(hasBeenCalled[0]);
    Assert.assertTrue(connectionResetThrown[0]);
  }

  @Test
  public void testRenameBlobIdempotencyWhereDstIsCopiedFromSomeOtherProcess()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    String srcDir = "/test1/test2/test3";
    fs.mkdirs(new Path(srcDir));
    fs.create(new Path(srcDir, "file1"));
    fs.create(new Path(srcDir, "file2"));

    fs.mkdirs(new Path("/test4"));

    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedStore = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(spiedStore).when(spiedFs).getAbfsStore();
    final AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());
    spiedStore.setClient(spiedClient);

    /*
     * First call to copyBlob for file1 will fail with connection-reset, but the
     * backend has got the call. Retry of that API would give 409 error.
     */
    boolean[] hasBeenCalled = new boolean[1];
    hasBeenCalled[0] = false;

    boolean[] connectionResetThrown = new boolean[1];
    connectionResetThrown[0] = false;

    AbfsClientTestUtil.setMockAbfsRestOperationForCopyBlobOperation(spiedClient,
        (spiedRestOp, actualCallMakerOp) -> {

          Mockito.doAnswer(answer -> {
            if (spiedRestOp.getUrl().toString().contains("file1")
                && !hasBeenCalled[0]) {
              hasBeenCalled[0] = true;
              fs.create(new Path("/randomDir/test3/file1"));
              fs.rename(new Path("/randomDir/test3/file1"),
                  new Path("/test4/test3/file1"));
              AbfsRestOperationTestUtil.addAbfsHttpOpProcessResponseMock(
                  spiedRestOp, (mockAbfsHttpOp, actualAbfsHttpOp) -> {
                    Mockito.doAnswer(sendRequestAnswer -> {
                          if (!connectionResetThrown[0]) {
                            connectionResetThrown[0] = true;
                            throw new SocketException("connection-reset");
                          }
                          spiedRestOp.signRequest(actualAbfsHttpOp,
                              sendRequestAnswer.getArgument(2));
                          actualAbfsHttpOp.sendRequest(
                              sendRequestAnswer.getArgument(0),
                              sendRequestAnswer.getArgument(1),
                              sendRequestAnswer.getArgument(2));
                          AbfsHttpOpTestUtil.setConnection(mockAbfsHttpOp,
                              actualAbfsHttpOp);
                          return mockAbfsHttpOp;
                        }).when(mockAbfsHttpOp)
                        .sendRequest(Mockito.nullable(byte[].class),
                            Mockito.anyInt(), Mockito.anyInt());

                    return mockAbfsHttpOp;
                  });
              Mockito.doCallRealMethod()
                  .when(spiedRestOp)
                  .execute(Mockito.any(TracingContext.class));
              spiedRestOp.execute(answer.getArgument(0));
              return spiedRestOp;
            } else {
              actualCallMakerOp.execute(answer.getArgument(0));
              AbfsRestOperationTestUtil.setResult(spiedRestOp,
                  actualCallMakerOp.getResult());
              return actualCallMakerOp;
            }
          }).when(spiedRestOp).execute(Mockito.any(TracingContext.class));
          return spiedRestOp;
        });

    spiedFs.setWorkingDirectory(new Path("/"));

    Boolean dstAlreadyThere = false;
    try {
      spiedFs.rename(new Path(srcDir), new Path("/test4"));
    } catch (RuntimeException ex) {
      if (ex.getMessage().contains(HttpURLConnection.HTTP_CONFLICT + "")) {
        dstAlreadyThere = true;
      }
    }
    Assert.assertTrue(dstAlreadyThere);
    Assert.assertTrue(hasBeenCalled[0]);
    Assert.assertTrue(connectionResetThrown[0]);
  }

  @Test
  public void testRenameLargeNestedDir() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    String dir = "/";
    for (int i = 0; i < 100; i++) {
      dir += ("dir" + i + "/");
      fs.mkdirs(new Path(dir));
    }
    fs.mkdirs(new Path("/dst"));
    fs.rename(new Path("/dir0"), new Path("/dst"));
    dir = "";
    for (int i = 0; i < 100; i++) {
      dir += ("dir" + i + "/");
      Assert.assertTrue("" + i, fs.exists(new Path("/dst/" + dir)));
    }
  }

  @Test
  public void testRenameDirWhenMarkerBlobIsAbsent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.mkdirs(new Path("/test1"));
    fs.mkdirs(new Path("/test1/test2"));
    fs.mkdirs(new Path("/test1/test2/test3"));
    fs.create(new Path("/test1/test2/test3/file"));

    fs.getAbfsClient()
        .deleteBlobPath(new Path("/test1/test2"),
            null, Mockito.mock(TracingContext.class));
    fs.mkdirs(new Path("/test4/test5"));
    fs.rename(new Path("/test4"), new Path("/test1/test2"));

    Assert.assertTrue(fs.exists(new Path("/test1/test2/test4/test5")));

    fs.mkdirs(new Path("/test6"));
    fs.rename(new Path("/test6"), new Path("/test1/test2/test4/test5"));
    Assert.assertTrue(fs.exists(new Path("/test1/test2/test4/test5/test6")));

    fs.getAbfsClient()
        .deleteBlobPath(new Path("/test1/test2/test4/test5/test6"),
            null, Mockito.mock(TracingContext.class));
    fs.mkdirs(new Path("/test7"));
    fs.create(new Path("/test7/file"));
    fs.rename(new Path("/test7"), new Path("/test1/test2/test4/test5/test6"));
    Assert.assertTrue(
        fs.exists(new Path("/test1/test2/test4/test5/test6/file")));
  }

  @Test
  public void testBlobRenameSrcDirHasNoMarker() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.create(new Path("/test1/test2/file1"));
    fs.getAbfsStore()
        .getClient()
        .deleteBlobPath(new Path("/test1"), null, Mockito.mock(TracingContext.class));
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore().getBlobProperty(new Path("/test1"),
              Mockito.mock(TracingContext.class));
    });
    fs.mkdirs(new Path("/test2"));
    fs.rename(new Path("/test1"), new Path("/test2"));
    Assert.assertTrue(fs.getAbfsStore()
        .getBlobProperty(new Path("/test2/test1"),
            Mockito.mock(TracingContext.class)).getIsDirectory());
  }

  @Test
  public void testCopyBlobTakeTime() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fileSystem);
    AzureBlobFileSystemStore store = Mockito.spy(fileSystem.getAbfsStore());
    Mockito.doReturn(store).when(fileSystem).getAbfsStore();
    AbfsClient client = store.getClient();
    AbfsClient spiedClient = Mockito.spy(client);
    store.setClient(spiedClient);

    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = Mockito.spy((AbfsRestOperation) answer.callRealMethod());
      AbfsHttpOperation httpOp = Mockito.spy(op.getResult());
      Mockito.doReturn(COPY_STATUS_PENDING).when(httpOp).getResponseHeader(
          HttpHeaderConfigurations.X_MS_COPY_STATUS);
      Mockito.doReturn(httpOp).when(op).getResult();
      return op;
    }).when(spiedClient).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
        Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    fileSystem.create(new Path("/test1/file"));
    fileSystem.rename(new Path("/test1/file"), new Path("/test1/file2"));
    Assert.assertTrue(fileSystem.exists(new Path("/test1/file2")));
    Mockito.verify(store, Mockito.times(1))
        .handleCopyInProgress(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(String.class));
  }

  @Test
  public void testCopyBlobTakeTimeAndEventuallyFail() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fileSystem);
    AzureBlobFileSystemStore store = Mockito.spy(fileSystem.getAbfsStore());
    Mockito.doReturn(store).when(fileSystem).getAbfsStore();
    AbfsClient client = store.getClient();
    AbfsClient spiedClient = Mockito.spy(client);
    store.setClient(spiedClient);

    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = Mockito.spy((AbfsRestOperation) answer.callRealMethod());
      AbfsHttpOperation httpOp = Mockito.spy(op.getResult());
      Mockito.doReturn(COPY_STATUS_PENDING).when(httpOp).getResponseHeader(
          HttpHeaderConfigurations.X_MS_COPY_STATUS);
      Mockito.doReturn(httpOp).when(op).getResult();
      return op;
    }).when(spiedClient).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
        Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = Mockito.spy((AbfsRestOperation) answer.callRealMethod());
      AbfsHttpOperation httpOp = Mockito.spy(op.getResult());
      Mockito.doReturn(COPY_STATUS_FAILED).when(httpOp).getResponseHeader(
          HttpHeaderConfigurations.X_MS_COPY_STATUS);
      Mockito.doReturn(httpOp).when(op).getResult();
      return op;
    }).when(spiedClient).getBlobProperty(Mockito.any(Path.class), Mockito.any(TracingContext.class));

    fileSystem.create(new Path("/test1/file"));
    Boolean copyBlobFailureCaught = false;
    try {
      fileSystem.rename(new Path("/test1/file"), new Path("/test1/file2"));
    } catch (AbfsRestOperationException e) {
      if (COPY_BLOB_FAILED.equals(e.getErrorCode())) {
        copyBlobFailureCaught = true;
      }
    }
    Assert.assertTrue(copyBlobFailureCaught);
    Assert.assertTrue(fileSystem.exists(new Path("/test1/file")));
    Mockito.verify(store, Mockito.times(1))
        .handleCopyInProgress(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(String.class));
  }

  @Test
  public void testCopyBlobTakeTimeAndEventuallyAborted() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fileSystem);
    AzureBlobFileSystemStore store = Mockito.spy(fileSystem.getAbfsStore());
    Mockito.doReturn(store).when(fileSystem).getAbfsStore();
    AbfsClient client = store.getClient();
    AbfsClient spiedClient = Mockito.spy(client);
    store.setClient(spiedClient);

    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = Mockito.spy((AbfsRestOperation) answer.callRealMethod());
      AbfsHttpOperation httpOp = Mockito.spy(op.getResult());
      Mockito.doReturn(COPY_STATUS_PENDING).when(httpOp).getResponseHeader(
          HttpHeaderConfigurations.X_MS_COPY_STATUS);
      Mockito.doReturn(httpOp).when(op).getResult();
      return op;
    }).when(spiedClient).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
        Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = Mockito.spy((AbfsRestOperation) answer.callRealMethod());
      AbfsHttpOperation httpOp = Mockito.spy(op.getResult());
      Mockito.doReturn(COPY_STATUS_ABORTED).when(httpOp).getResponseHeader(
          HttpHeaderConfigurations.X_MS_COPY_STATUS);
      Mockito.doReturn(httpOp).when(op).getResult();
      return op;
    }).when(spiedClient).getBlobProperty(Mockito.any(Path.class), Mockito.any(TracingContext.class));

    fileSystem.create(new Path("/test1/file"));
    Boolean copyBlobFailureCaught = false;
    try {
      fileSystem.rename(new Path("/test1/file"), new Path("/test1/file2"));
    } catch (AbfsRestOperationException e) {
      if (COPY_BLOB_ABORTED.equals(e.getErrorCode())) {
        copyBlobFailureCaught = true;
      }
    }
    Assert.assertTrue(copyBlobFailureCaught);
    Assert.assertTrue(fileSystem.exists(new Path("/test1/file")));
    Mockito.verify(store, Mockito.times(1))
        .handleCopyInProgress(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(String.class));
  }

  @Test
  public void testCopyBlobTakeTimeAndBlobIsDeleted() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fileSystem);
    AzureBlobFileSystemStore store = Mockito.spy(fileSystem.getAbfsStore());
    String srcFile = "/test1/file";
    String dstFile = "/test1/file2";
    Mockito.doReturn(store).when(fileSystem).getAbfsStore();
    AbfsClient client = store.getClient();
    AbfsClient spiedClient = Mockito.spy(client);
    store.setClient(spiedClient);

    Mockito.doAnswer(answer -> {
      AbfsRestOperation op = Mockito.spy((AbfsRestOperation) answer.callRealMethod());
      fileSystem.delete(new Path(dstFile), false);
      AbfsHttpOperation httpOp = Mockito.spy(op.getResult());
      Mockito.doReturn(COPY_STATUS_PENDING).when(httpOp).getResponseHeader(
          HttpHeaderConfigurations.X_MS_COPY_STATUS);
      Mockito.doReturn(httpOp).when(op).getResult();
      return op;
    }).when(spiedClient).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
        Mockito.nullable(String.class), Mockito.any(TracingContext.class));

    fileSystem.create(new Path(srcFile));


    Assert.assertFalse(fileSystem.rename(new Path(srcFile), new Path(dstFile)));
    Assert.assertFalse(fileSystem.exists(new Path(dstFile)));
  }

  @Test
  public void testParallelCopy() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.create(new Path("/src"));
    boolean[] dstBlobAlreadyThereExceptionReceived = new boolean[1];
    dstBlobAlreadyThereExceptionReceived[0] = false;
    AtomicInteger threadsCompleted = new AtomicInteger(0);
    new Thread(() -> {
      parallelCopyRunnable(fs, dstBlobAlreadyThereExceptionReceived,
          threadsCompleted);
    }).start();
    new Thread(() -> {
      parallelCopyRunnable(fs, dstBlobAlreadyThereExceptionReceived,
          threadsCompleted);
    }).start();
    while (threadsCompleted.get() < 2) ;
    Assert.assertTrue(dstBlobAlreadyThereExceptionReceived[0]);
  }

  private void parallelCopyRunnable(final AzureBlobFileSystem fs,
      final boolean[] dstBlobAlreadyThereExceptionReceived,
      final AtomicInteger threadsCompleted) {
    try {
      fs.getAbfsClient().copyBlob(new Path("/src"),
          new Path("/dst"), null, Mockito.mock(TracingContext.class));
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HttpURLConnection.HTTP_CONFLICT) {
        dstBlobAlreadyThereExceptionReceived[0] = true;
      }
    } catch (
        AzureBlobFileSystemException e) {
    }
    threadsCompleted.incrementAndGet();
  }

  @Test
  public void testCopyAfterSourceHasBeenDeleted() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.create(new Path("/src"));
    fs.getAbfsStore()
        .getClient()
        .deleteBlobPath(new Path("/src"), null, Mockito.mock(TracingContext.class));
    Boolean srcBlobNotFoundExReceived = false;
    try {
      fs.getAbfsStore()
          .copyBlob(new Path("/src"), new Path("/dst"),
              null, Mockito.mock(TracingContext.class));
    } catch (AbfsRestOperationException ex) {
      if (ex.getStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
        srcBlobNotFoundExReceived = true;
      }
    }
    Assert.assertTrue(srcBlobNotFoundExReceived);
  }

  @Test
  public void testParallelRenameForAtomicDirShouldFail() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("/hbase/dir1"));
    fs.create(new Path("/hbase/dir1/file1"));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    store.setClient(client);
    AtomicBoolean leaseAcquired = new AtomicBoolean(false);
    AtomicBoolean exceptionOnParallelRename = new AtomicBoolean(false);
    AtomicBoolean parallelThreadDone = new AtomicBoolean(false);
    Mockito.doAnswer(answer -> {
          AbfsRestOperation op = (AbfsRestOperation) answer.callRealMethod();
          leaseAcquired.set(true);
          while (!parallelThreadDone.get()) ;
          return op;
        })
        .when(client)
        .acquireBlobLease(Mockito.anyString(), Mockito.anyInt(),
            Mockito.any(TracingContext.class));

    new Thread(() -> {
      while (!leaseAcquired.get()) ;
      try {
        fs.rename(new Path("/hbase/dir1/file1"), new Path("/hbase/dir2/"));
      } catch (Exception e) {
        if (e.getCause() instanceof AbfsRestOperationException
            && ((AbfsRestOperationException) e.getCause()).getStatusCode()
            == HttpURLConnection.HTTP_CONFLICT) {
          exceptionOnParallelRename.set(true);
        }
      } finally {
        parallelThreadDone.set(true);
      }
    }).start();
    fs.rename(new Path("/hbase/dir1/file1"), new Path("/hbase/dir2/"));
    while (!parallelThreadDone.get()) ;
    Assert.assertTrue(exceptionOnParallelRename.get());
  }

  @Test
  public void testParallelAppendToFileBeingCopiedInAtomicDirectory()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("/hbase/dir1"));
    fs.create(new Path("/hbase/dir1/file1"));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    store.setClient(client);
    AtomicBoolean copyOfSrcFile = new AtomicBoolean(false);
    AtomicBoolean parallelAppendDone = new AtomicBoolean(false);
    AtomicBoolean exceptionCaught = new AtomicBoolean(false);

    Mockito.doAnswer(answer -> {
          answer.callRealMethod();
          if ("/hbase/dir1/file1".equalsIgnoreCase(
              ((Path) answer.getArgument(0)).toUri().getPath())) {
            copyOfSrcFile.set(true);
            while (!parallelAppendDone.get()) ;
          }
          return null;
        })
        .when(store)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));

    FSDataOutputStream outputStream = fs.append(new Path("/hbase/dir1/file1"));

    new Thread(() -> {
      while (!copyOfSrcFile.get()) ;
      try {
        byte[] bytes = new byte[4 * ONE_MB];
        new Random().nextBytes(bytes);
        outputStream.write(bytes);
        outputStream.hsync();
      } catch (Exception e) {
        if (e.getCause() instanceof AbfsRestOperationException
            && ((AbfsRestOperationException) e.getCause()).getStatusCode()
            == HttpURLConnection.HTTP_PRECON_FAILED) {
          exceptionCaught.set(true);
        }
      } finally {
        parallelAppendDone.set(true);
      }
    }).start();

    fs.rename(new Path("/hbase/dir1"), new Path("/hbase/dir2"));

    while (!parallelAppendDone.get()) ;
    Assert.assertTrue(exceptionCaught.get());
  }

  @Test
  public void testParallelBlobLeaseOnChildBlobInRenameSrcDir()
      throws Exception {
    assumeNonHnsAccountBlobEndpoint(getFileSystem());
    AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration()));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Path srcDirPath = new Path("/hbase/testDir");
    fs.mkdirs(srcDirPath);
    fs.create(new Path(srcDirPath, "file1"));
    fs.create(new Path(srcDirPath, "file2"));
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = store.getClient();
    AbfsClient spiedClient = Mockito.spy(client);
    store.setClient(spiedClient);

    fs.getAbfsClient()
        .acquireBlobLease("/hbase/testDir/file2", -1,
            Mockito.mock(TracingContext.class));

    AbfsLease[] leases = new AbfsLease[1];
    Mockito.doAnswer(answer -> {
          String path = answer.getArgument(0);
          AbfsLease lease = (AbfsLease) answer.callRealMethod();
          if (srcDirPath.toUri().getPath().equalsIgnoreCase(path)) {
            lease = Mockito.spy(lease);
            leases[0] = lease;
          }
          return lease;
        })
        .when(store)
        .getBlobLease(Mockito.anyString(), Mockito.nullable(Integer.class),
            Mockito.any(TracingContext.class));

    Boolean renameFailed = false;
    try {
      fs.rename(srcDirPath, new Path("/hbase/newDir"));
    } catch (Exception e) {
      renameFailed = true;
    }

    Assertions.assertThat(renameFailed).isTrue();
    Mockito.verify(leases[0], Mockito.times(1)).free();
  }

  @Test
  public void testParallelCreateNonRecursiveToFilePartOfAtomicDirectoryInRename()
      throws Exception {
    Configuration configuration = Mockito.spy(getRawConfiguration());
    configuration.set(FS_AZURE_LEASE_CREATE_NON_RECURSIVE, "true");
    FileSystem fsCreate = FileSystem.newInstance(configuration);
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("/hbase/dir1"));
    fs.create(new Path("/hbase/dir1/file1"));
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    fs.getAbfsStore().setClient(client);
    AtomicBoolean leaseAcquired = new AtomicBoolean(false);
    AtomicBoolean parallelCreateDone = new AtomicBoolean(false);
    AtomicBoolean exceptionCaught = new AtomicBoolean(false);
    AtomicBoolean parallelRenameDone = new AtomicBoolean(false);

    Mockito.doAnswer(answer -> {
          AbfsRestOperation op = (AbfsRestOperation) answer.callRealMethod();
          leaseAcquired.set(true);
          while(!parallelCreateDone.get());
          return op;
        })
        .when(client)
        .acquireBlobLease(Mockito.anyString(), Mockito.anyInt(),
            Mockito.any(TracingContext.class));

    new Thread(() -> {
      try {
        fs.rename(new Path("/hbase/dir1"), new Path("/hbase/dir2"));
      } catch (Exception e) {} finally {
        parallelRenameDone.set(true);
      }
    }).start();

    Path createNewFilePath = new Path("/hbase/dir1/file2");
      while (!leaseAcquired.get()) ;
      try {
        fsCreate.createFile(createNewFilePath)
            .overwrite(false)
            .replication((short) 1)
            .bufferSize(1024)
            .blockSize(1024)
            .build();
      } catch (AbfsRestOperationException e) {
        if (e.getStatusCode()
            == HttpURLConnection.HTTP_CONFLICT) {
          exceptionCaught.set(true);
        }
      } finally {
        parallelCreateDone.set(true);
      }


    while (!parallelRenameDone.get()) ;
    Assert.assertTrue(exceptionCaught.get());
  }

  @Test
  public void testBlobRenameOfDirectoryHavingNeighborWithSamePrefix()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.mkdirs(new Path("/testDir/dir"));
    fs.mkdirs(new Path("/testDir/dirSamePrefix"));
    fs.create(new Path("/testDir/dir/file1"));
    fs.create(new Path("/testDir/dir/file2"));

    fs.create(new Path("/testDir/dirSamePrefix/file1"));
    fs.create(new Path("/testDir/dirSamePrefix/file2"));

    fs.rename(new Path("/testDir/dir"), new Path("/testDir/dir2"));

    Assertions.assertThat(fs.exists(new Path("/testDir/dirSamePrefix/file1")))
        .isTrue();
    Assertions.assertThat(fs.exists(new Path("/testDir/dir/file1")))
        .isFalse();
    Assertions.assertThat(fs.exists(new Path("/testDir/dir/file2")))
        .isFalse();
    Assertions.assertThat(fs.exists(new Path("/testDir/dir/")))
        .isFalse();
  }

  @Test
  public void testBlobRenameCancelRenewTimerForLeaseTakenInAtomicRename()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration()));
    assumeNonHnsAccountBlobEndpoint(fs);
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();

    fs.mkdirs(new Path("/hbase/dir"));
    fs.create(new Path("/hbase/dir/file1"));
    fs.create(new Path("/hbase/dir/file2"));

    final List<AbfsBlobLease> leases = new ArrayList<>();
    Mockito.doAnswer(answer -> {
          AbfsBlobLease lease = Mockito.spy(
              (AbfsBlobLease) answer.callRealMethod());
          leases.add(lease);
          return lease;
        })
        .when(store)
        .getBlobLease(Mockito.anyString(), Mockito.nullable(Integer.class),
            Mockito.any(TracingContext.class));

    fs.rename(new Path("/hbase/dir"), new Path("/hbase/dir2"));

    Assertions.assertThat(leases).hasSize(3);
    for (AbfsBlobLease lease : leases) {
      Mockito.verify(lease, Mockito.times(1)).cancelTimer();
    }
  }

  @Test
  public void testBlobRenameServerReturnsOneBlobPerList() throws  Exception {
    assumeNonHnsAccountBlobEndpoint(getFileSystem());
    AzureBlobFileSystem fs = (AzureBlobFileSystem) Mockito.spy(FileSystem.newInstance(getRawConfiguration()));
    fs.mkdirs(new Path("/testDir/"));
    fs.create(new Path("/testDir/file1"));
    fs.create(new Path("/testDir/file2"));

    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = fs.getAbfsClient();
    AbfsClient spiedClient = Mockito.spy(client);
    store.setClient(spiedClient);
    Mockito.doReturn(store).when(fs).getAbfsStore();
    Mockito.doAnswer(answer -> {
      String marker = answer.getArgument(0);
      String prefix = answer.getArgument(1);
      String delimeter = answer.getArgument(2);
      Integer count = answer.getArgument(3);
      TracingContext tracingContext = answer.getArgument(4);
      AbfsRestOperation op = client.getListBlobs(marker, prefix, delimeter, 1, tracingContext);
      return op;
    }).when(spiedClient).getListBlobs(Mockito.nullable(String.class),
        Mockito.nullable(String.class), Mockito.nullable(String.class),
        Mockito.nullable(Integer.class), Mockito.any(TracingContext.class));

    fs.rename(new Path("/testDir"), new Path("/testDir1"));
    Assertions.assertThat(fs.exists(new Path("/testDir"))).isFalse();
    Assertions.assertThat(fs.exists(new Path("/testDir1"))).isTrue();
    Assertions.assertThat(fs.exists(new Path("/testDir1/file1"))).isTrue();
    Assertions.assertThat(fs.exists(new Path("/testDir1/file2"))).isTrue();
  }

  @Test
  public void testBlobAtomicRenameSrcAndDstAreNotLeftLeased() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path("/"));
    fs.create(new Path("/hbase/dir1/blob1"));
    fs.create(new Path("/hbase/dir1/blob2"));
    fs.rename(new Path("/hbase/dir1/"), new Path("/hbase/dir2"));
    fs.create(new Path("/hbase/dir1/blob1"));
    byte[] bytes = new byte[4 * ONE_MB];
    new Random().nextBytes(bytes);
    try (FSDataOutputStream os = fs.append(new Path("hbase/dir2/blob1"))) {
      os.write(bytes);
    }
  }

  /**
   * Test to assert that the CID in src marker blob copy and delete contains the
   * total number of blobs operated in the rename directory.
   * Also, to assert that all operations in the rename-directory flow have same
   * primaryId and opType.
   */
  @Test
  public void testRenameSrcDirDeleteEmitDeletionCountInClientRequestId()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    Assume.assumeTrue(getPrefixMode(fs) == PrefixMode.BLOB);
    String dirPathStr = "/testDir/dir1";
    fs.mkdirs(new Path(dirPathStr));
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final int iter = i;
      Future future = executorService.submit(() -> {
        return fs.create(new Path("/testDir/dir1/file" + iter));
      });
      futures.add(future);
    }

    for (Future future : futures) {
      future.get();
    }
    executorService.shutdown();


    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = Mockito.spy(store.getClient());
    store.setClient(client);

    final TracingHeaderValidator tracingHeaderValidator
        = new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.RENAME, true, 0);
    fs.registerListener(tracingHeaderValidator);

    Mockito.doAnswer(answer -> {
          Mockito.doAnswer(copyAnswer -> {
                if (dirPathStr.equalsIgnoreCase(
                    ((Path) copyAnswer.getArgument(0)).toUri().getPath())) {
                  tracingHeaderValidator.setOperatedBlobCount(11);
                  return copyAnswer.callRealMethod();
                }
                return copyAnswer.callRealMethod();
              })
              .when(client)
              .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
                  Mockito.nullable(String.class),
                  Mockito.any(TracingContext.class));

          Mockito.doAnswer(deleteAnswer -> {
                if (dirPathStr.equalsIgnoreCase(
                    ((Path) deleteAnswer.getArgument(0)).toUri().getPath())) {
                  Object result = deleteAnswer.callRealMethod();
                  tracingHeaderValidator.setOperatedBlobCount(null);
                  return result;
                }
                return deleteAnswer.callRealMethod();
              })
              .when(client)
              .deleteBlobPath(Mockito.any(Path.class),
                  Mockito.nullable(String.class),
                  Mockito.any(TracingContext.class));

          return answer.callRealMethod();
        })
        .when(store)
        .rename(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.any(
                RenameAtomicityUtils.class), Mockito.any(TracingContext.class));

    fs.rename(new Path(dirPathStr), new Path("/dst/"));
  }

  /**
   * Test to assert that the rename resume from FileStatus uses the same
   * {@link TracingContext} object used by the initial ListStatus call.
   * Also assert that last rename's copy and delete API call would push count
   * of blobs operated in resume flow in clientRequestId.
   */
  @Test
  public void testBlobRenameResumeWithListStatus() throws Exception {
    assumeNonHnsAccountBlobEndpoint(getFileSystem());
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    store.setClient(client);

    renameFailureSetup(fs, client);
    AtomicInteger copied = assertTracingContextOnRenameResumeProcess(fs, store,
        client, FSOperationType.LISTSTATUS);

    fs.listStatus(new Path("/hbase"));
    fs.registerListener(null);
    Assertions.assertThat(fs.exists(new Path("/hbase/testDir2"))).isTrue();
    Assertions.assertThat(copied.get()).isGreaterThan(0);
  }

  /**
   * Test to assert that the rename resume from FileStatus uses the same
   * {@link TracingContext} object used by the initial GetFileStatus call.
   * Also assert that last rename's copy and delete API call would push count
   * of blobs operated in resume flow in clientRequestId.
   */
  @Test
  public void testBlobRenameResumeWithGetFileStatus() throws Exception {
    assumeNonHnsAccountBlobEndpoint(getFileSystem());
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    store.setClient(client);

    renameFailureSetup(fs, client);
    AtomicInteger copied = assertTracingContextOnRenameResumeProcess(fs, store,
        client, FSOperationType.GET_FILESTATUS);

    intercept(FileNotFoundException.class, () -> {
      fs.getFileStatus(new Path("/hbase/testDir"));
    });
    Assertions.assertThat(fs.exists(new Path("/hbase/testDir2"))).isTrue();
    Assertions.assertThat(copied.get()).isGreaterThan(0);
  }

  private void renameFailureSetup(final AzureBlobFileSystem fs,
      final AbfsClient client)
      throws Exception {
    fs.mkdirs(new Path("/hbase/testDir"));
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final int iter = i;
      futures.add(executorService.submit(
          () -> fs.create(new Path("/hbase/testDir/file" + iter))));
    }

    for (Future future : futures) {
      future.get();
    }

    AbfsRestOperation op = client.acquireBlobLease("/hbase/testDir/file5", -1,
        Mockito.mock(TracingContext.class));
    String leaseId = op.getResult()
        .getResponseHeader(HttpHeaderConfigurations.X_MS_LEASE_ID);

    Map<String, String> pathLeaseIdMap = new ConcurrentHashMap<>();
    AtomicBoolean leaseCanBeTaken = new AtomicBoolean(true);
    Mockito.doAnswer(answer -> {
          if (!leaseCanBeTaken.get()) {
            throw new RuntimeException();
          }
          AbfsRestOperation abfsRestOperation
              = (AbfsRestOperation) answer.callRealMethod();
          pathLeaseIdMap.put(answer.getArgument(0),
              abfsRestOperation.getResult().getResponseHeader(X_MS_LEASE_ID));
          return abfsRestOperation;
        })
        .when(client)
        .acquireBlobLease(Mockito.anyString(), Mockito.anyInt(),
            Mockito.any(TracingContext.class));

    intercept(Exception.class, () -> {
      fs.rename(new Path("/hbase/testDir"), new Path("/hbase/testDir2"));
    });

    leaseCanBeTaken.set(false);
    for (Map.Entry<String, String> entry : pathLeaseIdMap.entrySet()) {
      try {
        client.releaseBlobLease(entry.getKey(), entry.getValue(),
            Mockito.mock(TracingContext.class));
      } catch (Exception e) {

      }
    }
    client.releaseBlobLease("/hbase/testDir/file5", leaseId,
        Mockito.mock(TracingContext.class));
    leaseCanBeTaken.set(true);
  }

  private AtomicInteger assertTracingContextOnRenameResumeProcess(final AzureBlobFileSystem fs,
      final AzureBlobFileSystemStore store,
      final AbfsClient client, final FSOperationType fsOperationType)
      throws IOException {
    final TracingHeaderValidator tracingHeaderValidator
        = new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), fsOperationType, true, 0);
    fs.registerListener(tracingHeaderValidator);

    AtomicInteger copied = new AtomicInteger(0);
    Mockito.doAnswer(answer -> {
          copied.incrementAndGet();
          Path path = answer.getArgument(0);
          if ("/hbase/testDir".equalsIgnoreCase(path.toUri().getPath())) {
            tracingHeaderValidator.setOperatedBlobCount(copied.get());
          }
          return answer.callRealMethod();
        })
        .when(store)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));

    /*
     * RenameAtomicUtil internally calls Filesystem's API  of read and delete
     * which would have different primaryIds. But once renameAtomicUtil has read
     * the RenamePending JSON, all the operations will use the same tracingContext
     * which was started by ListStatus or GetFileStatus operation.
     * This is the reason why the validation is disabled until the RenameAtomicUtil
     * object reads the JSON.
     * The filesystem's delete API called in RenameAtomicUtils.cleanup create a
     * new TracingContext object with a new primaryRequestId and also updates the
     * new primaryRequestId in the listener object of FileSystem. Therefore, once,
     * cleanup method is completed, the listener is explicitly updated with the
     * primaryRequestId it was using before the RenameAtomicUtils object was created.
     */
    Mockito.doAnswer(answer -> {
          final String primaryRequestId = ((TracingContext) answer.getArgument(
              1)).getPrimaryRequestId();
          tracingHeaderValidator.setDisableValidation(true);
          RenameAtomicityUtils renameAtomicityUtils = Mockito.spy(
              (RenameAtomicityUtils) answer.callRealMethod());
          Mockito.doAnswer(cleanupAnswer -> {
            tracingHeaderValidator.setDisableValidation(true);
            cleanupAnswer.callRealMethod();
            tracingHeaderValidator.setDisableValidation(false);
            tracingHeaderValidator.updatePrimaryRequestID(primaryRequestId);
            return null;
          }).when(renameAtomicityUtils).cleanup(Mockito.any(Path.class));
          tracingHeaderValidator.setDisableValidation(false);
          return renameAtomicityUtils;
        })
        .when(fs)
        .getRenameAtomicityUtilsForRedo(Mockito.any(Path.class),
            Mockito.any(TracingContext.class));

    Mockito.doAnswer(answer -> {
          answer.callRealMethod();
          tracingHeaderValidator.setOperatedBlobCount(null);
          return null;
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    return copied;
  }
}
