package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsAuthFailureRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;
import org.apache.hadoop.util.functional.InvocationRaisingIOE;

public abstract class OperationExecuteOrchestrator {
  protected  URL url;
  protected String method;
  protected List<AbfsHttpHeader> requestHeaders;
  protected AbfsClient abfsClient;

  private static final Logger LOG = LoggerFactory.getLogger(OperationExecuteOrchestrator.class);

  public OperationExecuteOrchestrator(final URL url,
      final String method,
      final List<AbfsHttpHeader> requestHeaders,
      final AbfsClient abfsClient) {
    this.url = url;
    this.method = method;
    this.requestHeaders = requestHeaders;
    this.abfsClient = abfsClient;
  }

  public void orchestrate(final OperationInvocationRaisingIOE operationInvocationRaisingIOE, final AbfsClient abfsClient, final AbfsRestOperation abfsRestOperation) throws IOException{
    int retryCount = 0;
    Boolean retryable = true;
    while (retryable) {
      try {
        operationInvocationRaisingIOE.apply();
        AbfsHttpOperation abfsHttpOperation = abfsRestOperation.getResult();
        if(abfsHttpOperation.getStatusCode() == HttpURLConnection.HTTP_PARTIAL) {
          handlePartialSuccess(abfsRestOperation);
          continue;
        }
        if(abfsHttpOperation.getStatusCode() >= HttpURLConnection.HTTP_BAD_REQUEST) {
          throw new AbfsRestOperationException(abfsHttpOperation.getStatusCode(), abfsHttpOperation.getStorageErrorCode(),
              abfsHttpOperation.getStorageErrorMessage(), null, abfsHttpOperation);
        }
        if(!abfsClient.getRetryPolicy().shouldRetry(retryCount,
            abfsHttpOperation.getStatusCode())) {
          retryable = false;
        }
        retryCount++;
      } catch (AbfsAuthFailureRestOperationException e) {
        throw e;
      }
      catch (UnknownHostException e) {
        if(!abfsClient.getRetryPolicy().shouldRetry(retryCount, -1)) {
          throw new InvalidAbfsRestOperationException(e);
        }
      } catch (IOException e) {
        if (e instanceof SocketException && "Connection reset".equals(
            e.getMessage())) {
          handleConnectionReset(abfsRestOperation);
        } else {
          if(!abfsClient.getRetryPolicy().shouldRetry(retryCount, -1)) {
            throw new InvalidAbfsRestOperationException(e);
          }
        }
      }
    }
  }

  private void handlePartialSuccess(final AbfsRestOperation abfsRestOperation) {

  }

  protected void handleConnectionReset(final AbfsRestOperation abfsRestOperation) {

  }

  public interface OperationInvocationRaisingIOE {
    public AbfsHttpOperation apply() throws IOException;
  }
}
