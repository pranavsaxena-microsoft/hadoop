package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AppendRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.abfsStreamHelpers.AbfsOutputStreamHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class MockAbfsOutputStream extends AbfsOutputStream {

  private  Boolean isFpConnectionException = false;
  public Map<String, Integer> helpersUsed = new HashMap<>();


  public MockAbfsOutputStream(final AbfsOutputStreamContext abfsOutputStreamContext)
      throws IOException {
    super(abfsOutputStreamContext);
  }

  public void induceFpConnectionException() {
    isFpConnectionException = true;
  }

  @Override
  protected AbfsRestOperation executeWrite(final String path,
      final byte[] buffer,
      final AppendRequestParameters reqParams,
      final String cachedSasToken,
      final TracingContext tracingContext,
      final AbfsOutputStreamHelper outputStreamHelper)
      throws AzureBlobFileSystemException {
    signalErrorConditionToMockClient();

    final String helperClassName = outputStreamHelper.getClass().getName();
    Integer currentCount = helpersUsed.get(helperClassName);
    currentCount = (currentCount == null) ? 1 : (currentCount + 1);
    helpersUsed.put(helperClassName, currentCount);

    return super.executeWrite(path, buffer, reqParams, cachedSasToken,
        tracingContext, outputStreamHelper);
  }

  private void signalErrorConditionToMockClient() {
    if(isFpConnectionException) {
      ((MockAbfsClient) getClient()).induceFpRestConnectionException();
    }
  }

  public AbfsSession getAbfsSession() {
    return super.getAbfsSession();
  }

  @Override
  protected Future<Void> executorServiceSubmit(final Callable callable) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      callable.call();
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }
}
