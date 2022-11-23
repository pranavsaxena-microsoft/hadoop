package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientFileThrottlingAnalyzer;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsClientThrottlingAnalyzer;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestIntercept;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestInterceptResult;

import static java.net.HttpURLConnection.HTTP_PARTIAL;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class ITestPartialReadPrefetch extends ITestPartialRead {

  private static final String TEST_PATH = "/testfile";

  private Logger LOG =
      LoggerFactory.getLogger(ITestPartialReadPrefetch.class);

  public ITestPartialReadPrefetch() throws Exception {
  }

  @Test
  public void test() throws Exception {
    int fileSize = 12 * ONE_MB;
    Path testPath = path(TEST_PATH);



    byte[] originalFile = setup(testPath, fileSize);
    getFileSystem().getAbfsStore().getAbfsConfiguration().setInputStreamLevelPrefetchDisable(true);
    getFileSystem().getAbfsStore().getAbfsConfiguration().setReadAheadEnabled(true);
    getFileSystem().getAbfsStore().getAbfsConfiguration().setReadAheadQueueDepth(2);


    final AzureBlobFileSystem fs = getFileSystem();

    ActualServerReadByte actualServerReadByte = new ActualServerReadByte(
        fileSize, originalFile);

    final AbfsClient originalClient = fs.getAbfsClient();

    FSDataInputStream inputStreamOriginal = fs.open(testPath);

    String[] path = new String[1];

    MockHttpOperationTestIntercept mockHttpOperationTestIntercept
        = new MockHttpOperationTestIntercept() {
      private int callCount = 0;

      @Override
      public MockHttpOperationTestInterceptResult intercept(final AbfsHttpOperation abfsHttpOperation,
          final byte[] buffer,
          final int offset,
          final int length) throws IOException {
        /*
         * 1. Check if server can handle the request parameters.
         * 2. return 1MB data to test-client.
         */

        MockHttpOperationTestInterceptResult
            mockHttpOperationTestInterceptResult
            = new MockHttpOperationTestInterceptResult();
        mockHttpOperationTestInterceptResult.setStatus(HTTP_PARTIAL);


        if(AbfsClientFileThrottlingAnalyzer.getAnalyzer(path[0]).suspendTime() <= 10000 ) {
          mockHttpOperationTestInterceptResult.setBytesRead(ONE_MB);
        } else {
          mockHttpOperationTestInterceptResult.setBytesRead(length);
          callActualServerAndAssertBehaviour(abfsHttpOperation, buffer, offset,
              length, actualServerReadByte, length, inputStreamOriginal);
        }

        callCount++;
        return mockHttpOperationTestInterceptResult;
      }

      public int getCallCount() {
        return callCount;
      }
    };


    setMocks(fs, originalClient, mockHttpOperationTestIntercept);

    MockAbfsClientThrottlingAnalyzer analyzerToBeAsserted = setReadAnalyzer();

    FSDataInputStream inputStream = fs.open(testPath);
    path[0] = ((AbfsInputStream)inputStream.getWrappedStream()).getPath();
    byte[] buffer = new byte[fileSize];
    for(int i=0;i<200;i++) {
      inputStream.read(0, buffer, 0, fileSize);
    }
    Assertions.assertThat(mockHttpOperationTestIntercept.getCallCount())
        .describedAs("Number of server calls is wrong")
        .isEqualTo(4);
    Assertions.assertThat(analyzerToBeAsserted.getFailedInstances().intValue())
        .describedAs(
            "Number of server calls counted as throttling case is incorrect")
        .isEqualTo(3);
  }


}
