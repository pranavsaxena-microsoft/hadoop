package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class FailureExperiment extends AbstractAbfsScaleTest {

  public FailureExperiment() throws Exception {
    super();
  }

  @Test
  public void breakIt() throws IOException, InterruptedException {

    final AzureBlobFileSystem fs = getFileSystem();
    int bufferSize = 4 * ONE_MB;
    final String TEST_PATH = "/testfile";
    final String TEST_PATH2 = "/testfile2";
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore()
        .getAbfsConfiguration();
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);

    final byte[] b1 = new byte[2 * bufferSize];
    new Random().nextBytes(b1);

    Path testPath = path(TEST_PATH);
    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b1);
    } finally {
      stream.close();
    }

    final byte[] b2 = new byte[2 * bufferSize];
    new Random().nextBytes(b2);

    Path testPath2 = path(TEST_PATH2);
    stream = fs.create(testPath2);
    try {
      stream.write(b2);
    } finally {
      stream.close();
    }


    FSDataInputStream i2 = fs.open(testPath2);


    Boolean assertion[] = new Boolean[1];
    assertion[0] = false;

    FSDataInputStream[] i1 = new FSDataInputStream[10];
    for (int i = 0; i < 5; i++) {
      i1[i] = fs.open(testPath);
    }

    while(true) {


      for (Integer i = 0; i < 5; i++) {
        final Integer iter = i;
        new Thread(() -> {
          try {
            byte[] b1i = new byte[4 * ONE_MB];
            i1[iter].read(0, b1i, 0, 4 * ONE_MB);
            i1[iter].close();
          } catch (Exception e) {

          }
        }).start();
      }
      new Thread(() -> {
        final byte[] b2i = new byte[8 * ONE_MB];
        try {
          i2.read(0, b2i, 0, 8 * ONE_MB);
          i2.close();
        } catch (Exception e) {

        } finally {
          for (int i = 0; i < 8 * ONE_MB; i++) {
            if (b2i[i] != b2[i]) {
              assertion[0] = true;
              throw new RuntimeException("Data corruption captured!!!");
            }
          }
        }
      }).start();
      Assert.assertFalse(assertion[0]);
      System.gc();
    }

  }


}
