package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class FastpathAbfsInputStreamHelper implements AbfsInputStreamHelper{

    private AbfsFastpathSession fastpathSession;
    private AbfsClient client;
    private String path;
    private String eTag;
    private TracingContext tracingContext;
    private Boolean shouldFastPathBeUsed = true;

    private ThreadLocal<AbfsFastpathSessionInfo> abfsFastpathSessionInfoThreadLocal = new ThreadLocal<>();

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
            abfsFastpathSessionInfoThreadLocal.set(fastpathSession.getCurrentAbfsFastpathSessionInfoCopy());
            return new AbfsReadFastpathServerCaller(this, client);
        }
        return null;
    }

    @Override
    public void postExecute() {
        final AbfsFastpathSessionInfo fastpathSessionInfo = abfsFastpathSessionInfoThreadLocal.get();
        if(fastpathSessionInfo != null) {
            fastpathSession.updateConnectionModeForFailures(fastpathSessionInfo.getConnectionMode());
        }
        abfsFastpathSessionInfoThreadLocal.remove();
    }

    @Override
    public void close() {
        fastpathSession.close();
    }

    AbfsFastpathSessionInfo getAbfsFastpathSessionInfo() {
        return abfsFastpathSessionInfoThreadLocal.get();
    }

    @Override
    public int rank() {
        return 1;
    }

    public void changeFastPathUseFlag(Boolean shouldFastPathBeUsed) {
        this.shouldFastPathBeUsed = shouldFastPathBeUsed;
    }
}
