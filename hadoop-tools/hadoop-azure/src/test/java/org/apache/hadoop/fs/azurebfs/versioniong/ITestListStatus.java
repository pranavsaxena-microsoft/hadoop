package org.apache.hadoop.fs.azurebfs.versioniong;

import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.utils.ChangeBlobAccountSettingUtis;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestListStatus extends
    AbstractAbfsIntegrationTest {


  public ITestListStatus() throws Exception {
    super();
  }

  public String getAccountNameBeforeDot() {
    return super.getAccountName().split("\\.")[0];
  }


  @Test
  public void test1() throws Exception {
    test(true, true, true, false);
  }

  @Test
  public void test2() throws Exception {
    test(true, true, true, true);
  }

  @Test
  public void test3() throws Exception {
    test(true, false, true, false);
  }

  @Test
  public void test4() throws Exception {
    test(true, false, true, true);
  }

  @Test
  public void test5() throws Exception {
    test(false, true, true, false);
  }

  @Test
  public void test6() throws Exception {
    test(false, true, true, true);
  }

  @Test
  public void test7() throws Exception {
    test(false, false, true, false);
  }

  @Test
  public void test8() throws Exception {
    test(false, false, true, true);
  }

  @Test
  public void test9() throws Exception {
    test(true, true, false, false);
  }

  @Test
  public void test10() throws Exception {
    test(true, true, false, true);
  }

  @Test
  public void test11() throws Exception {
    test(true, false, false, false);
  }

  @Test
  public void test12() throws Exception {
    test(true, false, false, true);
  }

  @Test
  public void test13() throws Exception {
    test(false, true, false, false);
  }

  @Test
  public void test14() throws Exception {
    test(false, true, false, true);
  }

  @Test
  public void test15() throws Exception {
    test(false, false, false, false);
  }

  @Test
  public void test16() throws Exception {
    test(false, false, false, true);
  }


  private void test(Boolean firstAppendVersioning,
      Boolean secondAppendVersioning,
      Boolean setSoftDelete,
      Boolean toDelete) throws Exception {
    FileSystem fs = getFileSystem();
    fs.mkdirs(new Path("/testDir"));
    FileStatus testDirStatus1 = fs.getFileStatus(new Path("/testDir"));
    OutputStream ps = fs.create(new Path("/testDir/test1"));
    byte[] bytes = new byte[1024];
    new Random().nextBytes(bytes);
    ps.write(bytes);
    ps.close();


    ChangeBlobAccountSettingUtis.change(
        getConfiguration().get("subscriptionId"), getAccountNameBeforeDot(),
        firstAppendVersioning, setSoftDelete, "pranavsaxena",
        getConfiguration());
    ps = fs.append(new Path("/testDir/test1"));
    bytes = new byte[1024];
    new Random().nextBytes(bytes);
    ps.write(bytes);
    ps.close();


    ChangeBlobAccountSettingUtis.change(
        getConfiguration().get("subscriptionId"), getAccountNameBeforeDot(),
        secondAppendVersioning, setSoftDelete, "pranavsaxena",
        getConfiguration());
    ps = fs.append(new Path("/testDir/test1"));
    bytes = new byte[1024];
    new Random().nextBytes(bytes);
    ps.write(bytes);
    ps.close();


    if (toDelete) {
      fs.delete(new Path("/testDir/test1"), true);
      intercept(FileNotFoundException.class,
          () -> fs.getFileStatus(new Path("/testDir/test1")));
    } else {
      FileStatus[] statuses = fs.listStatus(new Path("/testDir"));
      Assertions.assertThat(statuses.length == 1).isTrue();
    }
    FileStatus testDirStatus2 = fs.getFileStatus(new Path("/testDir"));
    Assertions.assertThat(testDirStatus1.getModificationTime())
        .isEqualTo(testDirStatus2.getModificationTime());
  }

}
