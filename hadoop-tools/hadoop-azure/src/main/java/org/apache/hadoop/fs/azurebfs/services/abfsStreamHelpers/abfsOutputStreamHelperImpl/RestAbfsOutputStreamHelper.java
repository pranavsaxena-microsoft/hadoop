package org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.abfsOutputStreamHelperImpl;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.AbfsOutputStreamHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class RestAbfsOutputStreamHelper implements AbfsOutputStreamHelper {

  private AbfsOutputStreamHelper nextHelper;
  private Boolean isNextHelperIsValid = true;

  public RestAbfsOutputStreamHelper() {
    nextHelper = new OptimizedRestAbfsOutputStreamHelper(this);
  }

  @Override
  public AbfsOutputStreamHelper getNext() {
    return nextHelper;
  }

  @Override
  public AbfsOutputStreamHelper getBack() {
    return null;
  }

  @Override
  public void setNextAsInvalid() {
    isNextHelperIsValid = false;
  }

  @Override
  public boolean shouldGoNext(final AbfsOutputStreamContext abfsOutputStreamContext) {
    return (abfsOutputStreamContext.isDefaultConnectionOnOptimizedRest()
        && nextHelper != null && isNextHelperIsValid);
  }

  @Override
  public void setNextAsValid() {
    isNextHelperIsValid = true;
  }

  @Override
  public AbfsRestOperation operate(final String path,
      final byte[] buffer,
      final AppendRequestParameters reqParams,
      final String cachedSasToken,
      final TracingContext tracingContext, final AbfsClient abfsClient) throws AzureBlobFileSystemException {
    return abfsClient.append(path, buffer, reqParams, cachedSasToken, tracingContext);
  }
}
