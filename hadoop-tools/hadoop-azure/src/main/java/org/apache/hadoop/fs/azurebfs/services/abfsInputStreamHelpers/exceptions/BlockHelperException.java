package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.exceptions;


import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

public class BlockHelperException extends AzureBlobFileSystemException {
    public BlockHelperException(String s) {
        super(s);
    }

  public BlockHelperException(final Throwable throwable) {
    super(throwable);
  }
}
