package org.apache.hadoop.fs.azurebfs.services.abfsInputStreamHelpers.exceptions;

import java.io.IOException;

public class BlockHelperException extends IOException {
    public BlockHelperException(String s) {
        super(s);
    }
}
