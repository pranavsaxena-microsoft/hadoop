package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

public class BlobProperty {
  private Boolean isDirectory;
  private Path path;
  private Boolean exist;
  private String url;

  private String copySourceUrl;
  private String copyId;
  private String copyStatus;
  private String statusDescription;

  private AzureBlobFileSystemException ex;

  BlobProperty() {

  }


  public Boolean getIsDirectory() {
    return isDirectory;
  }

  public Boolean exists() {
    return exist;
  }

  public AzureBlobFileSystemException getFailureException() {
    return ex;
  }

  public Path getPath() {
    return path;
  }

  public Path getBlobDstPath(Path dstBlobPath) {
    return null;
  }

  public String getUrl() {
    return url;
  }

  public String getCopySourceUrl() {
    return copySourceUrl;
  }

  public String getCopyId() {
    return copyId;
  }

  public String getCopyStatus() {
    return copyStatus;
  }

  public String getStatusDescription() {
    return statusDescription;
  }
}
