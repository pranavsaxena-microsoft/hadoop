package org.apache.hadoop.fs.azurebfs.services;

import java.util.Objects;

public class BlockWithId {
  String blockId;
  long offset;

  BlockWithId(final String blockId, final long offset) {
    this.blockId = blockId;
    this.offset = offset;
  }

  String getBlockId() {
    return blockId;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof BlockWithId)) {
      return false;
    }
    BlockWithId customObj = (BlockWithId) obj;
    return Objects.equals(blockId, customObj.blockId)
        && Objects.equals(offset, customObj.offset);
  }

  // Override the hashCode() method
  @Override
  public int hashCode() {
    return Objects.hash(blockId, offset);
  }
}

