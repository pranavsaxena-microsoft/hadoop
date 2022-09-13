package org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public interface AbfsOutputStreamHelper {

  public boolean shouldGoNext(final AbfsOutputStreamContext abfsOutputStreamContext);

  public AbfsOutputStreamHelper getNext();

  public AbfsOutputStreamHelper getBack();

  public void setNextAsInvalid();

  public AbfsRestOperation operate(final String path,
      final byte[] buffer,
      AppendRequestParameters reqParams,
      final String cachedSasToken,
      TracingContext tracingContext)
      throws AzureBlobFileSystemException;
}
