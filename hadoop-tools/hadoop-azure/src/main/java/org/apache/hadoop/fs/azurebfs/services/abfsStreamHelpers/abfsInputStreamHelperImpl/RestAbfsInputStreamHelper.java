package org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.abfsInputStreamHelperImpl;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamRequestContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.AbfsInputStreamHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class RestAbfsInputStreamHelper implements AbfsInputStreamHelper {

  private AbfsInputStreamHelper nextHelper;
  private Boolean isNextHelperValid = true;

  public RestAbfsInputStreamHelper() {
    nextHelper = new OptimizedRestAbfsInputStreamHelper(this);
  }

  @Override
  public boolean shouldGoNext(final AbfsInputStreamContext abfsInputStreamContext) {
    if (abfsInputStreamContext == null) {
      return false;
    }
    return (abfsInputStreamContext.isDefaultConnectionOnOptimizedRest()
        && nextHelper != null && isNextHelperValid);
  }

  @Override
  public void setNextAsValid() {
    isNextHelperValid = true;
  }

  @Override
  public AbfsInputStreamHelper getNext() {
    return nextHelper;
  }

  @Override
  public AbfsInputStreamHelper getBack() {
    return null;
  }

  @Override
  public void setNextAsInvalid() {
    isNextHelperValid = false;
  }

  @Override
  public Boolean isAbfsSessionRequired() {
    return false;
  }

  @Override
  public Boolean explicitPreFetchReadAllowed() {
    return true;
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
    return abfsClient.read(path, bytes, sasToken, readRequestParameters,
        tracingContext);
  }
}
