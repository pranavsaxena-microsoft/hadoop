package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers;

import java.io.IOException;

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public interface AbfsInputStreamHelper {
    public boolean shouldGoNext();
    public AbfsInputStreamHelper getNext();
    public AbfsInputStreamHelper getBack();
    public void setNextAsInvalid();
    public AbfsRestOperation operate(String path, byte[] bytes, String sasToken, ReadRequestParameters readRequestParameters,
                                     TracingContext tracingContext, AbfsClient abfsClient)
        throws IOException;
    public Boolean explicitPreFetchReadAllowed();
}
