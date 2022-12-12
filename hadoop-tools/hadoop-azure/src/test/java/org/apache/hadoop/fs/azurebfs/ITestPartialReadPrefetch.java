package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;

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

public class ITestPartialReadPrefetch extends AbstractAbfsIntegrationTest {

  private static final String TEST_PATH = "/testfile";
  private static final String TEST_PATH2= "/testfile2";

  private Logger LOG =
      LoggerFactory.getLogger(ITestPartialReadPrefetch.class);

  public ITestPartialReadPrefetch() throws Exception {
  }

  @Test
  public void test() throws Exception {
    int fileSize = 12 * ONE_MB;
    Path testPath = path(TEST_PATH);
    Path testPath2 = path(TEST_PATH2);



    byte[] originalFile = PartialReadUtils.setup(testPath, fileSize, getFileSystem());
    byte[] originalFile2 = PartialReadUtils.setup(testPath2, fileSize, getFileSystem());

    getFileSystem().getAbfsStore().getAbfsConfiguration().setInputStreamLevelPrefetchDisable(true);
    getFileSystem().getAbfsStore().getAbfsConfiguration().setReadAheadEnabled(true);
    getFileSystem().getAbfsStore().getAbfsConfiguration().setReadAheadQueueDepth(2);


    final AzureBlobFileSystem fs = getFileSystem();

    PartialReadUtils.ActualServerReadByte actualServerReadByte = new PartialReadUtils.ActualServerReadByte(
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

        //https://pranavsaxenahns.dfs.core.windows.net/abfs-testcontainer-d2ac257d-5e0d-4c16-bbf1-6870f26815dd/testfile91916b5d8ca3?timeout=90
        //abfsHttpOperation.getMaskedUrl().split("/")[4].split("\\?")[0]

        String pathInStack = "/" + abfsHttpOperation.getMaskedUrl().split("/")[4].split("\\?")[0];

        if(pathInStack.equalsIgnoreCase(path[0])) {
          mockHttpOperationTestInterceptResult.setBytesRead(ONE_MB);
        } else {
          mockHttpOperationTestInterceptResult.setBytesRead(length);

          Mockito.doCallRealMethod()
              .when(abfsHttpOperation)
              .processResponse(Mockito.nullable(byte[].class),
                  Mockito.nullable(Integer.class), Mockito.nullable(Integer.class));

          abfsHttpOperation.processResponse(buffer, offset, length);
          mockHttpOperationTestInterceptResult.setBytesRead(length);
        }

        callCount++;
        return mockHttpOperationTestInterceptResult;
      }

      public int getCallCount() {
        return callCount;
      }
    };


    PartialReadUtils.setMocks(fs, originalClient, mockHttpOperationTestIntercept);

    MockAbfsClientThrottlingAnalyzer analyzerToBeAsserted = PartialReadUtils.setReadAnalyzer(
        fs.getAbfsStore().getAbfsConfiguration());

    FSDataInputStream inputStreamThrottled = fs.open(testPath);
    FSDataInputStream inputStreamNonThrottled = fs.open(testPath2);

    path[0] = ((AbfsInputStream)inputStreamThrottled.getWrappedStream()).getPath();
    byte[] buffer = new byte[fileSize];

    while(AbfsClientFileThrottlingAnalyzer.getAnalyzer(path[0]) == null ||
        AbfsClientFileThrottlingAnalyzer.getAnalyzer(path[0]).suspendTime() < 10000l) {
      inputStreamThrottled.read(0, buffer, 0, fileSize);
      inputStreamNonThrottled.read(0,buffer, 0, fileSize);
    }
    int callsDone = mockHttpOperationTestIntercept.getCallCount();
    inputStreamThrottled.read(0, buffer, 0, fileSize);
//    Assertions.assertThat(mockHttpOperationTestIntercept.getCallCount()).isEqualTo(callsDone + 12);

    inputStreamNonThrottled.read(new byte[4 * ONE_MB], 0, 4 * ONE_MB);
//    Thread.sleep(10000l);
    callsDone = mockHttpOperationTestIntercept.getCallCount();
    inputStreamNonThrottled.read(new byte[8 * ONE_MB], 0, 8 * ONE_MB);
    Assertions.assertThat(mockHttpOperationTestIntercept.getCallCount()).isEqualTo(callsDone);


//    Assertions.assertThat(mockHttpOperationTestIntercept.getCallCount())
//        .describedAs("Number of server calls is wrong")
//        .isEqualTo(4);
//    Assertions.assertThat(analyzerToBeAsserted.getFailedInstances().intValue())
//        .describedAs(
//            "Number of server calls counted as throttling case is incorrect")
//        .isEqualTo(3);
  }


}
