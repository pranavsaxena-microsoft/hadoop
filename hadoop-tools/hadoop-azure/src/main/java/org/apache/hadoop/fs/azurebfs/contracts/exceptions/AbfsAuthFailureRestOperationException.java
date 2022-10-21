package org.apache.hadoop.fs.azurebfs.contracts.exceptions;

public class AbfsAuthFailureRestOperationException extends AbfsRestOperationException{

  public AbfsAuthFailureRestOperationException(
      final int statusCode,
      final String errorCode,
      final String errorMessage,
      final Exception innerException) {
    super(statusCode, errorCode, errorMessage, innerException);
  }
}
