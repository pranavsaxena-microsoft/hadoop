package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsConnectionMode;
import org.apache.hadoop.fs.azurebfs.services.AbfsFastpathRestResponseHeaderBlock;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamRequestContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsSessionData;
import org.apache.hadoop.fs.azurebfs.services.ReadBufferManager;
import org.apache.hadoop.fs.azurebfs.services.ThreadBasedMessageQueue;
import org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.exceptions.BlockHelperException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastpathRestAbfsInputStreamHelper
    implements AbfsInputStreamHelper {

  private AbfsInputStreamHelper nextHelper;

  private AbfsInputStreamHelper prevHelper;

  private static List<ReadAheadByteInfo> readAheadByteInfoList
      = new ArrayList<>();


  private static final Logger LOG = LoggerFactory.getLogger(FastpathRestAbfsInputStreamHelper.class);

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
      AbfsClient abfsClient,
      final AbfsInputStreamRequestContext abfsInputStreamRequestContext)
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
          AbfsFastpathRestResponseHeaderBlock
              abfsFastpathRestResponseHeaderBlock
              = (AbfsFastpathRestResponseHeaderBlock) ThreadBasedMessageQueue.getData(
              this);
          final AbfsSessionData currentSessionData
              = readRequestParameters.getAbfsSessionData();
          final AbfsSessionData sessionDataForNextRequest = new AbfsSessionData(
              abfsFastpathRestResponseHeaderBlock.getSessionToken(),
              OffsetDateTime.parse(
                  abfsFastpathRestResponseHeaderBlock.getSessionExpiry(),
                  DateTimeFormatter.RFC_1123_DATE_TIME),
              currentSessionData.getConnectionMode());
          abfsInputStreamRequestContext.setAbfsSessionData(
              sessionDataForNextRequest);

          Long nextSize = Math.min(
              abfsInputStreamRequestContext.getBufferSize(),
              (abfsInputStreamRequestContext.getLen()
                  + abfsInputStreamRequestContext.getCurrentOffset()) - (
                  readRequestParameters.getStoreFilePosition()
                      + readRequestParameters.getReadLength()));

          if (nextSize == 0) {
            if(abfsInputStreamRequestContext.getCurrentOffset().compareTo(abfsInputStreamRequestContext.getStartOffset()) != 0) {
              return null;
            }
            int readAheadCount
                = abfsInputStreamRequestContext.getMaxReadAhead();
            readAheadCount--;
            if (readAheadCount > 0) {
              nextSize = Math.min(abfsInputStreamRequestContext.getBufferSize(),
                  abfsInputStreamRequestContext.getContentLength() - (
                      readRequestParameters.getStoreFilePosition()
                          + readRequestParameters.getReadLength()));
              abfsInputStreamRequestContext.setMaxReadAhead(readAheadCount);
              abfsInputStreamRequestContext.setCurrentOffset(
                  readRequestParameters.getStoreFilePosition()
                      + readRequestParameters.getReadLength());
              abfsInputStreamRequestContext.setStartOffset(
                  readRequestParameters.getStoreFilePosition()
                      + readRequestParameters.getReadLength());
              abfsInputStreamRequestContext.setLen(nextSize);
              ReadBufferManager.getBufferManager()
                  .queueReadAhead(
                      abfsInputStreamRequestContext.getAbfsInputStream(),
                      readRequestParameters.getStoreFilePosition()
                          + readRequestParameters.getReadLength(),
                      nextSize.intValue(), tracingContext,
                      abfsInputStreamRequestContext);
            }
          } else {
            abfsInputStreamRequestContext.setCurrentOffset(
                readRequestParameters.getStoreFilePosition()
                    + readRequestParameters.getReadLength());
            abfsInputStreamRequestContext.setLen(
                abfsInputStreamRequestContext.getLen() - nextSize);
            ReadBufferManager.getBufferManager()
                .queueReadAhead(
                    abfsInputStreamRequestContext.getAbfsInputStream(),
                    readRequestParameters.getStoreFilePosition()
                        + readRequestParameters.getReadLength(),
                    nextSize.intValue(), tracingContext,
                    abfsInputStreamRequestContext);
          }
          return null;

//          ReadAheadByteInfo readAheadByteInfo = getValidReadAheadByteInfo(
//              readRequestParameters.getBufferOffset());
//          int nextPossibleRetries = 3; // TODO: add via config
//          if (readAheadByteInfo != null) {
//            readAheadByteInfoList.remove(readAheadByteInfo);
//            nextPossibleRetries = readAheadByteInfo.readAheadNextPossibleCount
//                - 1;
//          }
//          if (nextPossibleRetries != 0) {
//            pushForReadAhead();//TODO: will add element in readAheadByteInfolist; populate inside ReadBufferManager
//          }
//          return null;
        }
      };
      final AbfsRestOperation op = abfsClient.read(path, bytes, sasToken,
          readRequestParameters, tracingContext, callable);
      return op;
    } catch (AzureBlobFileSystemException e) {
      if (readRequestParameters.isOptimizedRestConnection()) {
        LOG.debug("Fallback: From OptimizedREST to Vanilla REST");
        tracingContext.setConnectionMode(
            AbfsConnectionMode.REST_CONN);
        readRequestParameters.getAbfsSessionData()
            .setConnectionMode(
                AbfsConnectionMode.REST_CONN);
        throw new BlockHelperException(e);
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

//  private ReadAheadByteInfo getValidReadAheadByteInfo(int requiredOffset) {
//    for (ReadAheadByteInfo readAheadByteInfo : readAheadByteInfoList) {
//      if (((readAheadByteInfo.offsetLastRead + readAheadByteInfo.len) >= (
//          requiredOffset - 1)) &&
//          readAheadByteInfo.offsetLastRead < requiredOffset) {
//        return readAheadByteInfo;
//      }
//    }
//    return null;
//  }

  class ReadAheadByteInfo {

    public Long offsetLastRead;

    public Long len;

    public int readAheadNextPossibleCount;
  }
}
