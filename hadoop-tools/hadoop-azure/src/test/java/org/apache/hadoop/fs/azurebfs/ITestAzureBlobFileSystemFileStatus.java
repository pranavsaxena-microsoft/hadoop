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

import java.io.IOException;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mockito.Mockito;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.Mockito.times;

/**
 * Test FileStatus.
 */
public class ITestAzureBlobFileSystemFileStatus extends
    AbstractAbfsIntegrationTest {
  private static final String DEFAULT_FILE_PERMISSION_VALUE = "640";
  private static final String DEFAULT_DIR_PERMISSION_VALUE = "750";
  private static final String DEFAULT_UMASK_VALUE = "027";
  private static final String FULL_PERMISSION = "777";

  private static final Path TEST_FILE = new Path("testFile");
  private static final Path TEST_FOLDER = new Path("testDir");

  public ITestAzureBlobFileSystemFileStatus() throws Exception {
    super();
  }

  @Test
  public void testEnsureStatusWorksForRoot() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();

    Path root = new Path("/");
    FileStatus[] rootls = fs.listStatus(root);
    assertEquals("root listing", 0, rootls.length);
  }

  @Test
  public void testFileStatusPermissionsAndOwnerAndGroup() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.getConf().set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, DEFAULT_UMASK_VALUE);
    touch(TEST_FILE);
    validateStatus(fs, TEST_FILE, false);
  }

  private FileStatus validateStatus(final AzureBlobFileSystem fs, final Path name, final boolean isDir)
      throws IOException {
    FileStatus fileStatus = fs.getFileStatus(name);

    String errorInStatus = "error in " + fileStatus + " from " + fs;

    if (!getIsNamespaceEnabled(fs)) {
      assertEquals(errorInStatus + ": owner",
              fs.getOwnerUser(), fileStatus.getOwner());
      assertEquals(errorInStatus + ": group",
              fs.getOwnerUserPrimaryGroup(), fileStatus.getGroup());
      assertEquals(new FsPermission(FULL_PERMISSION), fileStatus.getPermission());
    } else {
      // When running with namespace enabled account,
      // the owner and group info retrieved from server will be digit ids.
      // hence skip the owner and group validation
      if (isDir) {
        assertEquals(errorInStatus + ": permission",
                new FsPermission(DEFAULT_DIR_PERMISSION_VALUE), fileStatus.getPermission());
      } else {
        assertEquals(errorInStatus + ": permission",
                new FsPermission(DEFAULT_FILE_PERMISSION_VALUE), fileStatus.getPermission());
      }
    }

    return fileStatus;
  }

  @Test
  public void testFolderStatusPermissionsAndOwnerAndGroup() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.getConf().set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, DEFAULT_UMASK_VALUE);
    fs.mkdirs(TEST_FOLDER);

    validateStatus(fs, TEST_FOLDER, true);
  }

  @Test
  public void testAbfsPathWithHost() throws IOException {
    AzureBlobFileSystem fs = this.getFileSystem();
    Path pathWithHost1 = new Path("abfs://mycluster/abfs/file1.txt");
    Path pathwithouthost1 = new Path("/abfs/file1.txt");

    Path pathWithHost2 = new Path("abfs://mycluster/abfs/file2.txt");
    Path pathwithouthost2 = new Path("/abfs/file2.txt");

    // verify compatibility of this path format
    fs.create(pathWithHost1);
    assertTrue(fs.exists(pathwithouthost1));

    fs.create(pathwithouthost2);
    assertTrue(fs.exists(pathWithHost2));

    // verify get
    FileStatus fileStatus1 = fs.getFileStatus(pathWithHost1);
    assertEquals(pathwithouthost1.getName(), fileStatus1.getPath().getName());

    FileStatus fileStatus2 = fs.getFileStatus(pathwithouthost2);
    assertEquals(pathWithHost2.getName(), fileStatus2.getPath().getName());
  }

  @Test
  public void testLastModifiedTime() throws IOException {
    AzureBlobFileSystem fs = this.getFileSystem();
    Path testFilePath = new Path("childfile1.txt");
    long createStartTime = System.currentTimeMillis();
    long minCreateStartTime = (createStartTime / 1000) * 1000 - 1;
    //  Dividing and multiplying by 1000 to make last 3 digits 0.
    //  It is observed that modification time is returned with last 3
    //  digits 0 always.
    fs.create(testFilePath);
    long createEndTime = System.currentTimeMillis();
    FileStatus fStat = fs.getFileStatus(testFilePath);
    long lastModifiedTime = fStat.getModificationTime();
    assertTrue("lastModifiedTime should be after minCreateStartTime",
        minCreateStartTime < lastModifiedTime);
    assertTrue("lastModifiedTime should be before createEndTime",
        createEndTime > lastModifiedTime);
  }

  @Test
  public void testFileStatusOnFileWithImplicitParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode()
    );

    Path testPath = new Path("a/b.txt");
    azcopyHelper.createFileUsingAzcopy(fs.makeQualified(testPath).toUri().getPath().substring(1));

    assertTrue("Parent directory is implicit.",
        BlobDirectoryStateHelper.isImplicitDirectory(testPath.getParent(), fs));

    // Assert getFileStatus Succeed on path
    FileStatus fileStatus = fs.getFileStatus(testPath);
    assertNotNull(fileStatus.getPath());
    assertFalse(fileStatus.isDirectory());
    assertNotEquals(0L, fileStatus.getLen());
  }

  @Test
  public void testFileStatusOnFileWithExplicitParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path("a/b.txt");
    fs.create(testPath);

    assertTrue("Parent directory is explicit.",
        BlobDirectoryStateHelper.isExplicitDirectory(testPath.getParent(), fs));

    FileStatus fileStatus = fs.getFileStatus(testPath);
    assertNotNull(fileStatus.getPath());
    assertFalse(fileStatus.isDirectory());
  }

  @Test
  public void testFileStatusOnImplicitDirWithImplicitParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode()
    );

    Path testPath = new Path("a/b");
    azcopyHelper.createFolderUsingAzcopy(fs.makeQualified(testPath).toUri().getPath().substring(1));

    assertTrue("Path is implicit.",
        BlobDirectoryStateHelper.isImplicitDirectory(testPath, fs));
    assertTrue("Parent directory is implicit.",
        BlobDirectoryStateHelper.isImplicitDirectory(testPath.getParent(), fs));

    // Assert that getFileStatus succeeds
    FileStatus fileStatus = fs.getFileStatus(testPath);
    assertNotNull(fileStatus.getPath());
    assertTrue(fileStatus.isDirectory());
    assertEquals(0L, fileStatus.getLen());
  }

  @Test
  public void testFileStatusOnImplicitDirWithExplicitParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode()
    );

    Path testPath = new Path("a/b");
    azcopyHelper.createFolderUsingAzcopy(fs.makeQualified(testPath).toUri().getPath().substring(1));
    fs.mkdirs(testPath.getParent());

    assertTrue("Path is implicit.",
        BlobDirectoryStateHelper.isImplicitDirectory(testPath, fs));
    assertTrue("Parent directory is explicit.",
        BlobDirectoryStateHelper.isExplicitDirectory(testPath.getParent(), fs));

    // Assert that getFileStatus succeeds
    FileStatus fileStatus = fs.getFileStatus(testPath);
    assertNotNull(fileStatus.getPath());
    assertTrue(fileStatus.isDirectory());
    assertEquals(0L, fileStatus.getLen());
  }

  @Test
  public void testFileStatusOnExplicitDirWithExplicitParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path("a/b");
    fs.mkdirs(testPath);

    assertTrue("Parent directory is explicit.",
        BlobDirectoryStateHelper.isExplicitDirectory(testPath.getParent(), fs));
    assertTrue("Path is explicit.",
        BlobDirectoryStateHelper.isExplicitDirectory(testPath, fs));

    // Assert that getFileStatus Succeeds
    FileStatus fileStatus = fs.getFileStatus(testPath);
    assertNotNull(fileStatus.getPath());
    assertTrue(fileStatus.isDirectory());
    assertEquals(0L, fileStatus.getLen());
  }

  @Test
  public void testFileStatusOnNonExistingPathWithExplicitParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path("a/b.txt");
    fs.mkdirs(testPath.getParent());

    assertTrue("Parent directory is explicit.",
        BlobDirectoryStateHelper.isExplicitDirectory(testPath.getParent(), fs));

    // assert that getFileStatus fails
    intercept(IOException.class,
        () -> fs.getFileStatus(testPath));
  }

  @Test
  public void testFileStatusOnNonExistingPathWithImplicitParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode()
    );

    Path testPath = new Path("a/b.txt");
    azcopyHelper.createFolderUsingAzcopy(fs.makeQualified(
        testPath.getParent()).toUri().getPath().substring(1));

    assertTrue("Parent directory is implicit.",
        BlobDirectoryStateHelper.isImplicitDirectory(testPath.getParent(), fs));

    // assert that getFileStatus Fails with IOException
    intercept(IOException.class,
        () -> fs.getFileStatus(testPath));
  }

  @Test
  public void testFileStatusOnRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("/");
    fs.setWorkingDirectory(new Path("/"));

    // Assert that getFileSus on root path succeed.
    FileStatus fileStatus = fs.getFileStatus(path);
    assertTrue(fileStatus.isDirectory());
    assertTrue(fileStatus.getLen() == 0L);
  }

  @Test
  public void testGetPathPropertyCalled() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();

    fs.create(new Path("/testGetPathProperty"));
    FileStatus fileStatus = fs.getFileStatus(new Path("/testGetPathProperty"));

    Assert.assertFalse(fileStatus.isDirectory());

    Mockito.verify(store, times(1)).getPathProperty(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(Boolean.class));
    Mockito.verify(store, times(0)).getListBlobs(Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class), Mockito.any(Integer.class),
            Mockito.any(Boolean.class));
  }

  @Test
  public void testGetPathPropertyCalledImplicit() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();

    createAzCopyDirectory(new Path("/testImplicitDirectory"));
    FileStatus fileStatus = fs.getFileStatus(new Path("/testImplicitDirectory"));

    Assert.assertTrue(fileStatus.isDirectory());

    Mockito.verify(store, times(1)).getPathProperty(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(Boolean.class));
    Mockito.verify(store, times(1)).getListBlobs(Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class), Mockito.any(Integer.class),
            Mockito.any(Boolean.class));
  }
}
