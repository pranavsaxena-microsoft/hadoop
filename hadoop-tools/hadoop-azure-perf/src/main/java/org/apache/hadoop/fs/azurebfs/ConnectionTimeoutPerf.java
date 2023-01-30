package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ConnectionTimeoutPerf {
  PerfTestAzureSetup setup = new PerfTestAzureSetup();

  private static String TEST_PATH = "/testfile";

  static Logger LOG =
      LoggerFactory.getLogger(ConnectionTimeoutPerf.class);

  public ConnectionTimeoutPerf() throws Exception {
    setup = new PerfTestAzureSetup();
    setup.setup();
  }

  public static void main(String[] args) throws Exception {
    ConnectionTimeoutPerf connectionTimeoutPerf = new ConnectionTimeoutPerf();
    AzureBlobFileSystem fs = connectionTimeoutPerf.setup.getFileSystem();
    Integer threadCount = Integer.parseInt(args[0]);
    int connTimeout = Integer.parseInt(args[1]);
    long runTime = 1* 60*1000l;// 10 min
    AbfsHttpOperation.setConnTimeout(connTimeout);
    final Boolean[] threadDone = new Boolean[threadCount];
    final AtomicInteger count = new AtomicInteger(0);

    final byte[] b = new byte[4* ONE_MB];
    new Random().nextBytes(b);

    Path testPath = connectionTimeoutPerf.setup.path(TEST_PATH);
    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b);
    } finally{
      stream.close();
    }

    Long start = new Date().toInstant().toEpochMilli();

    for(int i=0;i<threadCount;i++) {
      threadDone[i] = false;
      final int currI = i;
      new Thread(() -> {
        while(new Date().toInstant().toEpochMilli() - start < runTime) {
          try {
            fs.open(testPath).read(0, new byte[4*ONE_MB], 0, 4 *ONE_MB);
            count.getAndIncrement();
          } catch (IOException e) {
//            throw new RuntimeException(e);
          }
        }
        threadDone[currI] = true;
      }).start();
    }
//    while(true) {
//      Boolean toBreak = true;
//      for(int i=0;i<threadCount;i++) {
//        if(threadDone[i] == false) {
//          toBreak = false;
//          break;
//        }
//      }
//      if(toBreak) {
//        break;
//      }
//    }

    while(new Date().toInstant().toEpochMilli() - start < runTime);

    LOG.info("CT_Seen: " + AbfsRestOperation.ctSeen);
    LOG.info("total ops: " + count.get());

  }
}
