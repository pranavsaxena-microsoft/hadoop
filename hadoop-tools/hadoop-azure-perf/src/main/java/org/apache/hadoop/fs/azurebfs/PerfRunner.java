package org.apache.hadoop.fs.azurebfs;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.DATA_BLOCKS_BUFFER;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class PerfRunner {
  PerfTestAzureSetup setup = new PerfTestAzureSetup();

  private static String TEST_PATH = "/testfile";

  static Logger LOG =
      LoggerFactory.getLogger(PerfRunner.class);

  public PerfRunner() throws Exception {
    setup = new PerfTestAzureSetup();
  }

  public void run(String[] args) throws Exception {
    Configuration configuration = setup.rawConfig;
    configuration.set(DATA_BLOCKS_BUFFER, args[0]);
    setup.setup();

    byte[] bytes = new byte[8 * ONE_MB];
    boolean[] done = new boolean[50];
    Long start = System.currentTimeMillis();
    for(int i=0;i<50;i++) {
      done[i] = false;
      final int fileId = i;
      new Thread(() -> {
        try {
          AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
              configuration);
          AbfsOutputStream os = (AbfsOutputStream) fs.create(
                  new Path("/testFile" + fileId))
              .getWrappedStream();
          new Random().nextBytes(bytes);


          for (int j = 0; j < 30; j++) {
            os.write(bytes);
          }
          os.close();

        } catch (Throwable ex) {

        } finally {
          done[fileId] = true;
        }
      }).start();
    }
    while(true) {
      boolean toContinue = false;
      for(int i=0;i<50;i++) {
        if(!done[i]) {
          toContinue = true;
          break;
        }
      }
      if(!toContinue) {
        break;
      }
    }
    System.out.println("Time taken: " + (System.currentTimeMillis() - start));
  }

  public static void main(String[] args) throws Exception {
    PerfRunner perfRunner = new PerfRunner();
    perfRunner.run(args);
  }

}
