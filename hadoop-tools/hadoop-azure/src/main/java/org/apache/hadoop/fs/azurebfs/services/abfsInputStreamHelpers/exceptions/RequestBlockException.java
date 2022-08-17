package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.exceptions;

import java.io.IOException;

public class RequestBlockException extends IOException {
  public RequestBlockException(String message) {
    super(message);
  }
}
