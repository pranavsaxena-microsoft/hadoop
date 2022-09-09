package org.apache.hadoop.fs.azurebfs.services;

public class AbfsOptimizedRestResponseHeaderBlock {
  private String sessionToken;
  private String sessionExpiry;

  public String getSessionToken() {
    return sessionToken;
  }

  public void setSessionToken(final String sessionToken) {
    this.sessionToken = sessionToken;
  }

  public String getSessionExpiry() {
    return sessionExpiry;
  }

  public void setSessionExpiry(final String sessionExpiry) {
    this.sessionExpiry = sessionExpiry;
  }
}
