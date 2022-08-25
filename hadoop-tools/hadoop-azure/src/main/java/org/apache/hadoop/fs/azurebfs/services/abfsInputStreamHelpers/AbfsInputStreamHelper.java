package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public interface AbfsInputStreamHelper {

  public boolean shouldGoNext(final AbfsInputStreamContext abfsInputStreamContext);

  public AbfsInputStreamHelper getNext();

  public AbfsInputStreamHelper getBack();

  public void setNextAsInvalid();

  public AbfsRestOperation operate(String path,
      byte[] bytes,
      String sasToken,
      ReadRequestParameters readRequestParameters,
      TracingContext tracingContext,
      AbfsClient abfsClient)
      throws AzureBlobFileSystemException;

  public Boolean explicitPreFetchReadAllowed();
}
