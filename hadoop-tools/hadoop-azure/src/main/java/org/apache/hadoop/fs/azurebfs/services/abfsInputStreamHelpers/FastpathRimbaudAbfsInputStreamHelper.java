package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class FastpathRimbaudAbfsInputStreamHelper implements  AbfsInputStreamHelper {

    private AbfsInputStreamHelper prevHelper;

    public FastpathRimbaudAbfsInputStreamHelper(AbfsInputStreamHelper prevHelper) {
        this.prevHelper = prevHelper;
    }

    @Override
    public boolean shouldGoNext() {
        return false;
    }

    @Override
    public AbfsInputStreamHelper getNext() {
        return null;
    }

    @Override
    public AbfsInputStreamHelper getBack() {
        return prevHelper;
    }

    @Override
    public Boolean explicitPreFetchReadAllowed() {
        return true;
    }

    @Override
    public void setNextAsInvalid() {
    }

    @Override
    public AbfsRestOperation operate(String path, byte[] bytes, String sasToken, ReadRequestParameters readRequestParameters, TracingContext tracingContext, AbfsClient abfsClient)
        throws AzureBlobFileSystemException {
      return abfsClient.read(path, bytes, sasToken, readRequestParameters, tracingContext);
    }
}
