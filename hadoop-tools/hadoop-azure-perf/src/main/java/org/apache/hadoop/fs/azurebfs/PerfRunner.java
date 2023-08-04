package org.apache.hadoop.fs.azurebfs;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

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

  /**
   *
   * args{} = {dataBlockBufferProp, parallelism, fileName, xmxPropInMB}
   */
  public void run(String[] args) throws Exception {
    Configuration configuration = setup.rawConfig;
    configuration.set(DATA_BLOCKS_BUFFER, args[0]);
    setup.setup();

    int parallelism = Integer.parseInt(args[1]);
    byte[] bytes = new byte[8 * ONE_MB];
    boolean[] done = new boolean[parallelism];
    Long start = System.currentTimeMillis();
    AtomicInteger outOfMemory = new AtomicInteger(0);
    AtomicInteger doneThreads = new AtomicInteger(0);
    CountDownLatch countDownLatch = new CountDownLatch(parallelism);
    for(int i=0;i<parallelism;i++) {
      done[i] = false;
      final int fileId = i;
      new Thread(() -> {
        int count = 300;
        AzureBlobFileSystem fs = null;
        try {
          fs = (AzureBlobFileSystem) FileSystem.newInstance(
              configuration);
          fs.create(
              new Path("/testFile" + fileId));
        } catch (Exception ex) {

        }
        while(true) {
          int completed = 0;
          try {
            AbfsOutputStream os = (AbfsOutputStream) fs.append(new Path("/testFile" + fileId)).getWrappedStream();
            new Random().nextBytes(bytes);


            for (int j = 0; j < count; j++) {
              os.write(bytes);
              completed++;
            }
            os.close();

          } catch (Throwable ex) {
            if(getMemoryException(ex)) {
              outOfMemory.incrementAndGet();
              System.out.println("OOM!!!!");
              System.gc();
              count-=completed;
              continue;
            }
          }
          break;
        }
        doneThreads.incrementAndGet();
        countDownLatch.countDown();
        System.out.println(doneThreads.get());
      }).start();
    }

    countDownLatch.await();

    Long timeTaken = (System.currentTimeMillis() - start);
    System.out.println("Time taken: " + timeTaken + "; OOM = " + outOfMemory);

    StringBuilder builder  = new StringBuilder();
    builder.append(args[0] + ", ").append(args[1] + ", ").append(args[3] + ", ").append(timeTaken + ", ").append(outOfMemory.get());
    List<String> line = new ArrayList<>();
    line.add(builder.toString());

    Files.write(Paths.get(args[2]), line, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
  }

  private boolean getMemoryException(Throwable ex) {
    while(ex != null) {
      if(ex instanceof  OutOfMemoryError) {
        return true;
      }
      ex = ex.getCause();
    }
    return false;
  }

  /**
   * Combination of diff parallelism vs diff heap memory: disk vs memory: -> count of OOM ex, time taken,
   * Write CSV
   * bash script to create diff combo and invoke jar cmd line.
   *
   * */
  public static void main(String[] args) throws Exception {
    PerfRunner perfRunner = new PerfRunner();
    perfRunner.run(args);
  }

}
