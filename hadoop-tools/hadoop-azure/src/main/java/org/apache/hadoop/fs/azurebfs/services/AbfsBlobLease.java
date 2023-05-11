package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_LEASE_ID;

public class AbfsBlobLease {
  private String leaseId;
  private Long leaseRenewLastEpoch;
  private final TracingContext tracingContext;
  private final AbfsClient client;
  private final String path;
  private final Integer ONE_MINUTE = 60;
  private final Long RENEW_TIME = 30 * 1_000L;
  private Boolean freed = false;

  public AbfsBlobLease(AbfsClient client, String path, TracingContext tracingContext) throws
      AzureBlobFileSystemException {
    this.client = client;
    this.path = path;
    this.tracingContext = tracingContext;
    AbfsRestOperation op = client.acquireBlobLease(path, ONE_MINUTE, tracingContext);
    extractLeaseInfo(op);
  }

  private void extractLeaseInfo(final AbfsRestOperation op) {
    leaseId = op.getResult().getResponseHeader(X_MS_LEASE_ID);
    leaseRenewLastEpoch = System.currentTimeMillis();
  }

  public String getLeaseId() {
    return leaseId;
  }

  public void renewIfRequired() throws AzureBlobFileSystemException {
    if(System.currentTimeMillis() - leaseRenewLastEpoch >= RENEW_TIME) {
      renew();
    }
  }

  private synchronized void renew() throws AzureBlobFileSystemException {
    if(System.currentTimeMillis() - leaseRenewLastEpoch < RENEW_TIME) {
      return;
    }
    AbfsRestOperation op = client.renewBlobLease(path, leaseId, tracingContext);
    extractLeaseInfo(op);
  }

  public synchronized void free() throws AzureBlobFileSystemException {
    if(freed) {
      return;
    }
    client.releaseBlobLease(path, leaseId, tracingContext);
    freed = true;
  }
}
