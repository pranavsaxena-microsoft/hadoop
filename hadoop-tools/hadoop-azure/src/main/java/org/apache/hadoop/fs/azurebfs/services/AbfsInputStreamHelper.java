package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.services.AbfsServerCaller.AbfsReadServerCaller;

public interface AbfsInputStreamHelper {
    public AbfsInputStreamHelper init(AbfsInputStream abfsInputStream);
    public AbfsReadServerCaller preExecute();
    public void postExecute();
    public int rank();
}
