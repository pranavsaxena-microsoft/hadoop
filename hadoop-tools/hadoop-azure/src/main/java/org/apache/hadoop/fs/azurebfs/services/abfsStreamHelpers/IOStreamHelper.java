package org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers;

import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.abfsInputStreamHelperImpl.RestAbfsInputStreamHelper;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.abfsOutputStreamHelperImpl.RestAbfsOutputStreamHelper;

public class IOStreamHelper {
  private static AbfsInputStreamHelper inputStreamHelper = new RestAbfsInputStreamHelper();
  private static AbfsOutputStreamHelper outputStreamHelper = new RestAbfsOutputStreamHelper();

  public static AbfsInputStreamHelper getInputStreamHelper() {
    return inputStreamHelper;
  }

  public static AbfsOutputStreamHelper getOutputStreamHelper() {
    return outputStreamHelper;
  }
}
