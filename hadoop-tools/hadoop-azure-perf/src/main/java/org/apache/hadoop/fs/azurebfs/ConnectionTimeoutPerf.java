package org.apache.hadoop.fs.azurebfs;

import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;

public class ConnectionTimeoutPerf {
  PerfTestAzureSetup setup = new PerfTestAzureSetup();

  public ConnectionTimeoutPerf() throws Exception {
  }

  public static void main(String[] args) throws Exception {
    ConnectionTimeoutPerf connectionTimeoutPerf = new ConnectionTimeoutPerf();
    AzureBlobFileSystem fs = connectionTimeoutPerf.setup.getFileSystem();
    Integer threadCount = Integer.parseInt(args[0]);
    int connTimeout = Integer.parseInt(args[1]);
    long runTime = 10* 60*1000l;// 10 min
    AbfsHttpOperation.setConnTimeout(connTimeout);
    final Boolean[] threadDone = new Boolean[threadCount];
    final Integer[] totalOps = new Integer[1];
    totalOps[0] = 0;

    final byte[] b = new byte[fileSize];
    new Random().nextBytes(b);

    Path testPath = perfTestSetup.path(TEST_PATH);
    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b);
    } finally{
      stream.close();
    }


    for(int i=0;i<threadCount;i++) {
      threadDone[i] = false;
      new Thread(() -> {

      }).start();
    }
    while(true) {
      Boolean toBreak = true;
      for(int i=0;i<threadCount;i++) {
        if(threadDone[i] == false) {
          toBreak = false;
          break;
        }
      }
      if(toBreak) {
        break;
      }
    }

    System.out.println("CT_Seen: " + AbfsRestOperation.ctSeen);
    System.out.println("total ops: " + totalOps[0]);

  }
}
