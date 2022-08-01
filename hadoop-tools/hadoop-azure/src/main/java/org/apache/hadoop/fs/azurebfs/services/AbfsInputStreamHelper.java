package org.apache.hadoop.fs.azurebfs.services;

public interface AbfsInputStreamHelper {
    public AbfsInputStreamHelper init(AbfsInputStream abfsInputStream);
    public AbfsReadServerCaller preExecute();
    public void postExecute();
    public int rank();
    public void close();
}
