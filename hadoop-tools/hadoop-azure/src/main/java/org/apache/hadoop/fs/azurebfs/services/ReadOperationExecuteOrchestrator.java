package org.apache.hadoop.fs.azurebfs.services;

public class ReadOperationExecuteOrchestrator extends OperationExecuteOrchestrator {

  @Override
  protected void handlePartialSuccess(final AbfsRestOperation abfsRestOperation)
      throws RetryableExecption {
      throw new RetryableExecption();
  }

  @Override
  protected void handleConnectionReset(final AbfsRestOperation abfsRestOperation) {
    super.handleConnectionReset(abfsRestOperation);
  }
}
