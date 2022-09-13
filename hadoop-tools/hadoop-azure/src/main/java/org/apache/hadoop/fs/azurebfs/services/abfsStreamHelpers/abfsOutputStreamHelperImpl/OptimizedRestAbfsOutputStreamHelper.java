package org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.abfsOutputStreamHelperImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStreamContext;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.AbfsOutputStreamHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class OptimizedRestAbfsOutputStreamHelper
    implements AbfsOutputStreamHelper {

  private AbfsOutputStreamHelper nextHelper;

  private AbfsOutputStreamHelper prevHelper;


  private static final Logger LOG = LoggerFactory.getLogger(
      OptimizedRestAbfsOutputStreamHelper.class);

  public OptimizedRestAbfsOutputStreamHelper(AbfsOutputStreamHelper abfsInputStreamHelper) {
    nextHelper = null;
    prevHelper = abfsInputStreamHelper;
  }

  @Override
  public boolean shouldGoNext(final AbfsOutputStreamContext abfsOutputStreamContext) {
    return false;
  }

  @Override
  public AbfsRestOperation operate(final String path,
      final byte[] buffer,
      final AppendRequestParameters reqParams,
      final String cachedSasToken,
      final TracingContext tracingContext) throws AzureBlobFileSystemException {
    return null;
  }

  @Override
  public AbfsOutputStreamHelper getNext() {
    return nextHelper;
  }

  @Override
  public AbfsOutputStreamHelper getBack() {
    return prevHelper;
  }

  @Override
  public void setNextAsInvalid() {
    nextHelper = null;
  }
}
