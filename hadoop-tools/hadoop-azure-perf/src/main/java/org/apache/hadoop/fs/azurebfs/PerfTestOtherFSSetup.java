package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class PerfTestOtherFSSetup extends PerfTestSetup {
    @Override
    public void setup() throws Exception {

    }

    @Override
    public void setBuffer(int bufferSize) throws IOException {

    }

    @Override
    public FileSystem getFileSystem() throws IOException {
        return null;
    }
}
