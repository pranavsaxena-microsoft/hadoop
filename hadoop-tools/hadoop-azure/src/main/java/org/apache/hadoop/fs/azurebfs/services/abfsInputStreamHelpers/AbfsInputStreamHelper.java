package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers;

import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.exceptions.BlockHelperException;

import java.io.IOException;

public interface AbfsInputStreamHelper {
    public boolean shouldGoNext();
    public AbfsInputStreamHelper getNext();
    public AbfsInputStreamHelper getBack();
    public void setNextAsInvalid();
    public AbfsRestOperation operate() throws BlockHelperException;
}
