package org.apache.hadoop.fs.azurebfs;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.ListBlobConsumer;
import org.apache.hadoop.fs.azurebfs.services.ListBlobProducer;
import org.apache.hadoop.fs.azurebfs.services.ListBlobQueue;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_MAX_CONSUMER_LAG;

public class ITestListBlobProducer extends AbstractAbfsIntegrationTest {

  public ITestListBlobProducer() throws Exception {
    super();
  }

  @Test
  public void testProducerWaitingForConsumerLagToGoDown() throws Exception {
    Configuration configuration = Mockito.spy(getRawConfiguration());
    configuration.set(FS_AZURE_MAX_CONSUMER_LAG, "10");
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        configuration);
    AbfsClient client = fs.getAbfsClient();
    AbfsClient spiedClient = Mockito.spy(client);
    fs.getAbfsStore().setClient(spiedClient);
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("/src"));
    for (int i = 0; i < 20; i++) {
      fs.create(new Path("/src/file" + i));
    }
    AtomicBoolean produced = new AtomicBoolean(true);

    AtomicInteger producedBlobs = new AtomicInteger(0);
    Mockito.doAnswer(answer -> {
          AbfsRestOperation op = client.getListBlobs(answer.getArgument(0),
              answer.getArgument(1), 1, answer.getArgument(3));
          producedBlobs.incrementAndGet();
          produced.set(true);
          return op;
        })
        .when(spiedClient)
        .getListBlobs(Mockito.nullable(String.class),
            Mockito.nullable(String.class), Mockito.nullable(Integer.class),
            Mockito.nullable(TracingContext.class));

    ListBlobQueue queue = new ListBlobQueue(null);
    ListBlobProducer producer = new ListBlobProducer("src/", spiedClient, queue,
        null, Mockito.mock(
        TracingContext.class));
    ListBlobConsumer consumer = new ListBlobConsumer(queue);
    while (producedBlobs.get() < 10) ;

    int producedBlobCount = producedBlobs.get();

    while (!consumer.isCompleted()) {
      produced.set(false);
      consumer.consume();
      while (!produced.get() && !queue.getIsCompleted()) ;
      if (!queue.getIsCompleted()) {
        Assert.assertEquals(producedBlobs.get() - 1, producedBlobCount);
      }
      producedBlobCount = producedBlobs.get();
    }

    Assert.assertTrue(producedBlobCount == 20);
  }
}
