package org.apache.hadoop.fs.azurebfs.versioniong;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzcopyHelper;
import org.apache.hadoop.fs.azurebfs.utils.ChangeBlobAccountSettingUtis;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestAppend extends
    AbstractAbfsIntegrationTest {


  public ITestAppend() throws Exception {
    super();
  }

  public String getAccountNameBeforeDot() {
    return super.getAccountName().split("\\.")[0];
  }


  @Test
  public void test1() throws Exception {
    test(true, true, true);
  }


  @Test
  public void test2() throws Exception {
    test(true, false, true);
  }


  @Test
  public void test3() throws Exception {
    test(false, true, true);
  }

  @Test
  public void test4() throws Exception {
    test(false, false, true);
  }


  @Test
  public void test5() throws Exception {
    test(true, true, false);
  }


  @Test
  public void test6() throws Exception {
    test(true, false, false);
  }


  @Test
  public void test7() throws Exception {
    test(false, true, false);
  }


  @Test
  public void test8() throws Exception {
    test(false, false, false);
  }


  private void test(Boolean firstAppendVersioning,
      Boolean secondAppendVersioning,
      Boolean setSoftDelete) throws Exception {
    FileSystem fileSystem = getFileSystem();
    testInternal(true, firstAppendVersioning, secondAppendVersioning, setSoftDelete);
    testInternal(false, firstAppendVersioning, secondAppendVersioning, setSoftDelete);
  }

  void createAzCopyDirectory(Path path) throws Exception {
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(), getFileSystemName(),  getFileSystem().getAbfsStore()
        .getAbfsConfiguration()
        .getRawConfiguration(), getFileSystem().getAbfsStore().getPrefixMode());
    azcopyHelper.createFolderUsingAzcopy(
        getFileSystem().makeQualified(path).toUri().getPath().substring(1));
  }

  /**
   * For creating files with implicit parents. Doesn't change already explicit
   * parents.
   */
  void createAzCopyFile(Path path) throws Exception {
    AzcopyHelper azcopyHelper = new AzcopyHelper(getAccountName(),
        getFileSystemName(), getFileSystem().getAbfsStore()
        .getAbfsConfiguration()
        .getRawConfiguration(), getFileSystem().getAbfsStore().getPrefixMode());
    azcopyHelper.createFileUsingAzcopy(
        getFileSystem().makeQualified(path).toUri().getPath().substring(1));
  }




  private void testInternal(Boolean createViaAzcopy, Boolean firstAppendVersioning,
      Boolean secondAppendVersioning,
      Boolean setSoftDelete) throws Exception {
    FileSystem fs = getFileSystem();
    if(createViaAzcopy) {
      createAzCopyDirectory(new Path("/testDir"));
    } else {
      fs.mkdirs(new Path("/testDir"));
    }
    FileStatus testDirStatus1 = fs.getFileStatus(new Path("/testDir"));
    final OutputStream ps;
    if(createViaAzcopy) {
      createAzCopyFile(new Path("/testDir/test1"));
      ps = fs.append(new Path("/testDir/test1"));
    } else {
      ps = fs.create(new Path("/testDir/test1"));
    }
    String str1 = "abc";
    ps.write(str1.getBytes(StandardCharsets.UTF_8));
    ps.close();


    ChangeBlobAccountSettingUtis.change(
        getConfiguration().get("subscriptionId"), getAccountNameBeforeDot(),
        firstAppendVersioning, setSoftDelete, "pranavsaxena",
        getConfiguration());
    OutputStream ps1;
    ps1 = fs.append(new Path("/testDir/test1"));
    String str2 = "def";
    ps1.write(str2.getBytes(StandardCharsets.UTF_8));
    ps1.close();


    ChangeBlobAccountSettingUtis.change(
        getConfiguration().get("subscriptionId"), getAccountNameBeforeDot(),
        secondAppendVersioning, setSoftDelete, "pranavsaxena",
        getConfiguration());

    String str3 = "ghi";
    ps1 = fs.append(new Path("/testDir/test1"));
    ps1.write(str3.getBytes(StandardCharsets.UTF_8));
    ps1.close();


    InputStream inputStream = fs.open(new Path("/testDir/test1"));
    String text = IOUtils.toString(inputStream, StandardCharsets.UTF_8);

    Assertions.assertThat(text).isEqualTo(str1 + str2 + str3);

    FileStatus testDirStatus2 = fs.getFileStatus(new Path("/testDir"));
    Assertions.assertThat(testDirStatus1.getModificationTime())
        .isEqualTo(testDirStatus2.getModificationTime());
    fs.delete(new Path("/testDir/"), true);
  }

}
