package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers;

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.exceptions.BlockHelperException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class RestAbfsInputStreamHelper implements AbfsInputStreamHelper {

    private AbfsInputStreamHelper nextHelper;

    public RestAbfsInputStreamHelper() {
        nextHelper = new FastpathRestAbfsInputStreamHelper(this);
    }

    @Override
    public boolean shouldGoNext() {
        return nextHelper != null;
    }

    @Override
    public AbfsInputStreamHelper getNext() {
        return nextHelper;
    }

    @Override
    public AbfsInputStreamHelper getBack() {
        return null;
    }

    @Override
    public void setNextAsInvalid() {
        nextHelper = null;
    }

    @Override
    public Boolean explicitPreFetchReadAllowed() {
        return true;
    }

    @Override
    public AbfsRestOperation operate(String path, byte[] bytes, String sasToken, ReadRequestParameters readRequestParameters, TracingContext tracingContext, AbfsClient abfsClient) throws BlockHelperException {
        return null;
    }
}
