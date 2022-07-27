package org.apache.hadoop.fs.azurebfs;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.utils.UriUtils;

import java.io.IOException;
import java.util.UUID;

public abstract class PerfTestSetup {

    private static final int SHORTENED_GUID_LEN = 12;

    public abstract void setup() throws Exception;
    public abstract FileSystem getFileSystem() throws IOException;

    public abstract void setBuffer(int bufferSize) throws IOException;



    /**
     * Create a path under the test path provided by
     * {@link #getTestPath()}.
     * @param filepath path string in
     * @return a path qualified by the test filesystem
     * @throws IOException IO problems
     */
    public Path path(String filepath) throws IOException {
        return getFileSystem().makeQualified(
                new Path(getTestPath(), getUniquePath(filepath)));
    }

    private Path getTestPath() {
        Path path = new Path(UriUtils.generateUniqueTestPath());
        return path;
    }


    /**
     * Generate a unique path using the given filepath.
     * @param filepath path string
     * @return unique path created from filepath and a GUID
     */
    private Path getUniquePath(String filepath) {
        if (filepath.equals("/")) {
            return new Path(filepath);
        }
        return new Path(filepath + StringUtils
                .right(UUID.randomUUID().toString(), SHORTENED_GUID_LEN));
    }
}
