package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsAuthFailureRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidAbfsRestOperationException;

public class OperationExecuteOrchestrator {
  public final void orchestrate(final OperationInvocationRaisingIOE operationInvocationRaisingIOE, final AbfsClient abfsClient, final AbfsRestOperation abfsRestOperation) throws IOException{
    int retryCount = 0;
    Boolean retryable = true;
    while (retryable) {
      try {
        operationInvocationRaisingIOE.apply();
        AbfsHttpOperation abfsHttpOperation = abfsRestOperation.getResult();
        if (abfsHttpOperation.getStatusCode()
            == HttpURLConnection.HTTP_PARTIAL) {
          handlePartialSuccess(abfsRestOperation);
          continue;
        }
        if (abfsHttpOperation.getStatusCode()
            >= HttpURLConnection.HTTP_BAD_REQUEST) {
          throw new AbfsRestOperationException(
              abfsHttpOperation.getStatusCode(),
              abfsHttpOperation.getStorageErrorCode(),
              abfsHttpOperation.getStorageErrorMessage(), null,
              abfsHttpOperation);
        }
        if (!abfsClient.getRetryPolicy().shouldRetry(retryCount,
            abfsHttpOperation.getStatusCode())) {
          retryable = false;
        }
      } catch (RetryableExecption e) {
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
      } finally {
        retryCount++;
      }
    }
  }

  protected void handlePartialSuccess(final AbfsRestOperation abfsRestOperation)
      throws RetryableExecption {

  }

  protected void handleConnectionReset(final AbfsRestOperation abfsRestOperation) {

  }

  public interface OperationInvocationRaisingIOE {
    public AbfsHttpOperation apply() throws IOException;
  }


  /**
   * To drive retry.
   * */
  public static class RetryableExecption extends Exception {

  }
}
