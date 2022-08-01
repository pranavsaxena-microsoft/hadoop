package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.services.AbfsServerCaller.AbfsReadFastpathServerCaller;
import org.apache.hadoop.fs.azurebfs.services.AbfsServerCaller.AbfsReadServerCaller;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class FastpathAbfsInputStreamHelper implements AbfsInputStreamHelper{

    private AbfsFastpathSession fastpathSession;
    private AbfsClient client;
    private String path;
    private String eTag;
    private TracingContext tracingContext;
    private Boolean shouldFastPathBeUsed = true;

    @Override
    public FastpathAbfsInputStreamHelper init(AbfsInputStream abfsInputStream) {
        client = abfsInputStream.getClient();
        path = abfsInputStream.getPath();
        eTag = abfsInputStream.getETag();
        tracingContext = abfsInputStream.getTracingContext();
        fastpathSession = new AbfsFastpathSession(client, path, eTag, tracingContext);
        return this;
    }

    @Override
    public AbfsReadServerCaller preExecute() {
        if(shouldFastPathBeUsed) {
            return new AbfsReadFastpathServerCaller(this);
        }
        return null;
    }

    @Override
    public void postExecute() {

    }

    @Override
    public int rank() {
        return 1;
    }

    public void changeFastPathUseFlag(Boolean shouldFastPathBeUsed) {
        this.shouldFastPathBeUsed = shouldFastPathBeUsed;
    }
}
