package org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.abfsInputStreamHelperImpl;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsFastpathSession;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamRequestContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsSession;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.AbfsInputStreamHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.services.AbfsSession.IO_SESSION_SCOPE.READ_ON_FASTPATH;

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
  public AbfsSession createAbfsSession(final AbfsClient abfsClient,
      final String path,
      final String eTag,
      final TracingContext tracingContext) {
    return new AbfsFastpathSession(READ_ON_FASTPATH, abfsClient, path, eTag, tracingContext);
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
