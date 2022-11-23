package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingIntercept;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingInterceptTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType;
import org.apache.hadoop.fs.azurebfs.services.MockAbfsClientThrottlingAnalyzer;
import org.apache.hadoop.fs.azurebfs.services.MockClassUtils;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestIntercept;
import org.apache.hadoop.fs.azurebfs.services.MockHttpOperationTestInterceptResult;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class PartialReadUtils{

  private PartialReadUtils() {

  }

  private static Logger LOG =
      LoggerFactory.getLogger(PartialReadUtils.class);

  static byte[] setup(final Path testPath, final int fileSize, AzureBlobFileSystem fs)
      throws IOException {
    final AbfsConfiguration abfsConfiguration = fs.getAbfsStore()
        .getAbfsConfiguration();
    final int bufferSize = 4 * ONE_MB;
    abfsConfiguration.setWriteBufferSize(bufferSize);
    abfsConfiguration.setReadBufferSize(bufferSize);
    abfsConfiguration.setReadAheadQueueDepth(0);

    final byte[] b = new byte[fileSize];
    new Random().nextBytes(b);

    FSDataOutputStream stream = fs.create(testPath);
    try {
      stream.write(b);
    } finally {
      stream.close();
    }
    return b;
  }

  static MockAbfsClientThrottlingAnalyzer setReadAnalyzer() {
    AbfsClientThrottlingIntercept intercept
        = AbfsClientThrottlingInterceptTestUtil.get();
    MockAbfsClientThrottlingAnalyzer readAnalyzer
        = new MockAbfsClientThrottlingAnalyzer("read");
    MockAbfsClientThrottlingAnalyzer analyzerToBeAsserted
        = (MockAbfsClientThrottlingAnalyzer) AbfsClientThrottlingInterceptTestUtil.setReadAnalyzer(
        intercept, readAnalyzer);
    return analyzerToBeAsserted;
  }

  @SuppressWarnings("unchecked") // suppressing unchecked since, className of List<AbfsHttpHeader> not possible and need to supply List.class
  static void setMocks(final AzureBlobFileSystem fs,
      final AbfsClient originalClient,
      final MockHttpOperationTestIntercept mockHttpOperationTestIntercept) {
    AbfsClient abfsClient = Mockito.spy(originalClient);
    MockClassUtils.mockAbfsClientGetAbfsRestOperation(
        (getRestOpMockInvocation, objects) -> {
          AbfsRestOperation abfsRestOperation
              = MockClassUtils.createAbfsRestOperation(
              getRestOpMockInvocation.getArgument(0,
                  AbfsRestOperationType.class),
              (AbfsClient) objects[0],
              getRestOpMockInvocation.getArgument(1, String.class),
              getRestOpMockInvocation.getArgument(2, URL.class),
              getRestOpMockInvocation.getArgument(3, List.class),
              getRestOpMockInvocation.getArgument(4, byte[].class),
              getRestOpMockInvocation.getArgument(5, Integer.class),
              getRestOpMockInvocation.getArgument(6, Integer.class),
              getRestOpMockInvocation.getArgument(7, String.class)
          );
          if (AbfsRestOperationType.ReadFile
              == getRestOpMockInvocation.getArgument(0,
              AbfsRestOperationType.class)) {
            AbfsRestOperation mockRestOp = Mockito.spy(abfsRestOperation);
            setMockAbfsRestOperation(mockHttpOperationTestIntercept,
                mockRestOp);
            return mockRestOp;
          }
          return abfsRestOperation;
        }, abfsClient);
    fs.getAbfsStore().setClient(abfsClient);
  }

  static void setMockAbfsRestOperation(final MockHttpOperationTestIntercept mockHttpOperationTestIntercept,
      final AbfsRestOperation mockRestOp) throws IOException {
    MockClassUtils.mockAbfsRestOperationGetHttpOperation(
        (getHttpOpInvocationMock, getHttpOpObjects) -> {
          AbfsRestOperation op
              = (AbfsRestOperation) getHttpOpObjects[0];
          AbfsHttpOperation httpOperation = new AbfsHttpOperation(
              op.getUrl(), op.getMethod(), op.getRequestHeaders());
          AbfsHttpOperation spiedOp = Mockito.spy(httpOperation);
          setMockAbfsRestOperation(mockHttpOperationTestIntercept, spiedOp);
          return spiedOp;
        }, mockRestOp);
  }

  static void setMockAbfsRestOperation(final MockHttpOperationTestIntercept mockHttpOperationTestIntercept,
      final AbfsHttpOperation spiedOp) throws IOException {
    MockClassUtils.mockAbfsHttpOperationProcessResponse(
        (processResponseInvokation, processResponseObjs) -> {
          byte[] buffer = processResponseInvokation.getArgument(0,
              byte[].class);
          int offset = processResponseInvokation.getArgument(1,
              Integer.class);
          int length = processResponseInvokation.getArgument(2,
              Integer.class);

          AbfsHttpOperation abfsHttpOperation
              = (AbfsHttpOperation) processResponseObjs[0];

          MockHttpOperationTestInterceptResult result
              = mockHttpOperationTestIntercept.intercept(
              abfsHttpOperation, buffer, offset, length);
          MockClassUtils.setHttpOpStatus(result.getStatus(),
              abfsHttpOperation);
          MockClassUtils.setHttpOpBytesReceived(
              result.getBytesRead(), abfsHttpOperation);
          if (result.getException() != null) {
            throw result.getException();
          }
          return null;
        }, spiedOp);
  }

  static void callActualServerAndAssertBehaviour(final AbfsHttpOperation mockHttpOperation,
      final byte[] buffer,
      final int offset,
      final int length,
      final ActualServerReadByte actualServerReadByte,
      final int byteLenMockServerReturn, FSDataInputStream inputStream)
      throws IOException {
    LOG.info("length: " + length + "; offset: " + offset);
    Mockito.doCallRealMethod()
        .when(mockHttpOperation)
        .processResponse(Mockito.nullable(byte[].class),
            Mockito.nullable(Integer.class), Mockito.nullable(Integer.class));
    LOG.info("buffeLen: " + buffer.length + "; offset: " + offset + "; len " + length);
    mockHttpOperation.processResponse(buffer, offset, length);
    int iterator = 0;
    int currPointer = actualServerReadByte.currPointer;
    while (actualServerReadByte.currPointer < actualServerReadByte.size
        && iterator < buffer.length) {
      actualServerReadByte.bytes[actualServerReadByte.currPointer++]
          = buffer[iterator++];
    }
    actualServerReadByte.currPointer = currPointer + byteLenMockServerReturn;
    Boolean isOriginalAndReceivedFileEqual = true;
    if (actualServerReadByte.currPointer == actualServerReadByte.size) {
      for (int i = 0; i < actualServerReadByte.size; i++) {
        if (actualServerReadByte.bytes[i]
            != actualServerReadByte.originalFile[i]) {
          isOriginalAndReceivedFileEqual = false;
          break;
        }
      }
      Assertions.assertThat(isOriginalAndReceivedFileEqual)
          .describedAs("Parsed data is not equal to original file")
          .isTrue();
    }
  }

  static class ActualServerReadByte {

    byte[] bytes;

    int size;

    int currPointer = 0;

    byte[] originalFile;

    ActualServerReadByte(int size, byte[] originalFile) {
      bytes = new byte[size];
      this.size = size;
      this.originalFile = originalFile;
    }
  }
}
