package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers;

import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.exceptions.BlockHelperException;

public class FastpathRestAbfsInputStreamHelper implements AbfsInputStreamHelper {

    private AbfsInputStreamHelper nextHelper;
    private AbfsInputStreamHelper prevHelper;


    public FastpathRestAbfsInputStreamHelper(AbfsInputStreamHelper abfsInputStreamHelper) {
        nextHelper = new FastpathRimbaudAbfsInputStreamHelper(this);
        prevHelper = abfsInputStreamHelper;
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
        return prevHelper;
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
