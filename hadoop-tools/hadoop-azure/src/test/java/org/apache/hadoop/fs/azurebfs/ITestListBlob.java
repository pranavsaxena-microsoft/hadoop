package org.apache.hadoop.fs.azurebfs;


import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class ITestListBlob extends
    AbstractAbfsIntegrationTest {

  public ITestListBlob() throws Exception {
    super();
  }

  @Test
  public void testListBlob() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    if(fs.getAbfsStore().getAbfsConfiguration().getIsNamespaceEnabledAccount() == Trilean.TRUE) {
      return;
    }
    int i=0;
    while(i<10) {
      fs.create(new Path("/dir/" + i));
      i++;
    }
    List<BlobProperty> blobProperties = fs.getAbfsStore().getDirectoryBlobProperty(new Path("dir"),
        Mockito.mock(TracingContext.class), 1000);
    Assertions.assertThat(blobProperties).hasSize(1000);
  }
}
