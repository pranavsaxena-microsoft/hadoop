package org.apache.hadoop.fs.azurebfs.services;

public enum BlockStatus {
  UNCOMMITTED,
  COMMITTED,
  SUCCESS,
  FAILED
}
