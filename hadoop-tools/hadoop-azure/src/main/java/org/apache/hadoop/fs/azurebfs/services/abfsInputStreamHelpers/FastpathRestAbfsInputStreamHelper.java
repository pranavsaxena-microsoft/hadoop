package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.ThreadBasedMessageQueue;
import org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.exceptions.RequestBlockException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

public class FastpathRestAbfsInputStreamHelper
    implements AbfsInputStreamHelper {

  private AbfsInputStreamHelper nextHelper;

  private AbfsInputStreamHelper prevHelper;

  private static List<ReadAheadByteInfo> readAheadByteInfoList
      = new ArrayList<>();


  public FastpathRestAbfsInputStreamHelper(AbfsInputStreamHelper abfsInputStreamHelper) {
    nextHelper = new FastpathRimbaudAbfsInputStreamHelper(this);
    prevHelper = abfsInputStreamHelper;
  }

  @Override
  public boolean shouldGoNext(final AbfsInputStreamContext abfsInputStreamContext) {
    return (abfsInputStreamContext.isDefaultConnectionOnFastpath()
        && nextHelper != null);
  }

  @Override
  public AbfsInputStreamHelper getNext() {
    return nextHelper;
  }

  @Override
  public AbfsInputStreamHelper getBack() {
    return prevHelper;
  }

  @Override
  public void setNextAsInvalid() {
    nextHelper = null;
  }

  @Override
  public AbfsRestOperation operate(String path,
      byte[] bytes,
      String sasToken,
      ReadRequestParameters readRequestParameters,
      TracingContext tracingContext,
      AbfsClient abfsClient)
      throws AzureBlobFileSystemException {
    try {
      Callable callable = new Callable() {
        private String uuid = UUID.randomUUID().toString();

        @Override
        public boolean equals(Object o) {
          return super.equals(o);
        }

        @Override
        public Object call() throws Exception {
          Object dataOverMessageQueue = ThreadBasedMessageQueue.getData(this);
          ReadAheadByteInfo readAheadByteInfo = getValidReadAheadByteInfo(
              readRequestParameters.getBufferOffset());
          int nextPossibleRetries = 3; // TODO: add via config
          if (readAheadByteInfo != null) {
            readAheadByteInfoList.remove(readAheadByteInfo);
            nextPossibleRetries = readAheadByteInfo.readAheadNextPossibleCount
                - 1;
          }
          if (nextPossibleRetries != 0) {
            pushForReadAhead();//TODO: will add element in readAheadByteInfolist; populate inside ReadBufferManager
          }
          return null;
        }
      };
      final AbfsRestOperation op = abfsClient.read(path, bytes, sasToken,
          readRequestParameters, tracingContext, callable);
      return op;
    } catch (IOException e) {
      if (e.getClass() == AzureBlobFileSystemException.class) {
        throw new RequestBlockException(e);
      }
      throw e;
    }
  }

  private void pushForReadAhead() {
  }


  @Override
  public Boolean explicitPreFetchReadAllowed() {
    return false;
  }

  private ReadAheadByteInfo getValidReadAheadByteInfo(int requiredOffset) {
    for (ReadAheadByteInfo readAheadByteInfo : readAheadByteInfoList) {
      if (((readAheadByteInfo.offsetLastRead + readAheadByteInfo.len) >= (
          requiredOffset - 1)) &&
          readAheadByteInfo.offsetLastRead < requiredOffset) {
        return readAheadByteInfo;
      }
    }
    return null;
  }

  class ReadAheadByteInfo {

    public Long offsetLastRead;

    public Long len;

    public int readAheadNextPossibleCount;
  }
}
