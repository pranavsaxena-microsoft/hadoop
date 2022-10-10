package org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.abfsInputStreamHelperImpl;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamRequestContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.AbfsInputStreamHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class FastpathRimbaudAbfsInputStreamHelper
    implements AbfsInputStreamHelper {

  private AbfsInputStreamHelper prevHelper;

  public FastpathRimbaudAbfsInputStreamHelper(AbfsInputStreamHelper prevHelper) {
    this.prevHelper = prevHelper;
  }

  @Override
  public boolean shouldGoNext(final AbfsInputStreamContext abfsInputStreamContext) {
    return false;
  }

  @Override
  public AbfsInputStreamHelper getNext() {
    return null;
  }

  @Override
  public AbfsInputStreamHelper getBack() {
    return prevHelper;
  }

  @Override
  public Boolean explicitPreFetchReadAllowed() {
    return true;
  }

  @Override
  public Boolean isAbfsSessionRequired() {
    return null;
  }

  @Override
  public void setNextAsValid() {

  }

  @Override
  public void setNextAsInvalid() {
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
