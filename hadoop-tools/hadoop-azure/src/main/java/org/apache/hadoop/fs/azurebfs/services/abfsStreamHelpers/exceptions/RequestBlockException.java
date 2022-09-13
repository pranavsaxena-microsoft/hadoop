package org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.exceptions;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

public class RequestBlockException extends AzureBlobFileSystemException {
  public RequestBlockException(String message) {
    super(message);
  }

  public RequestBlockException(final Throwable throwable) {
    super(throwable);
  }
}
