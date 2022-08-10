package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers;

import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.exceptions.BlockHelperException;

public class RestAbfsInputStreamHelper implements AbfsInputStreamHelper {

    private AbfsInputStreamHelper nextHelper;

    public RestAbfsInputStreamHelper() {
        nextHelper = new FastpathRestAbfsInputStreamHelper(this);
    }

    @Override
    public boolean shouldGoNext() {
        return false;
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
    public AbfsRestOperation operate() throws BlockHelperException {
        return null;
    }
}
