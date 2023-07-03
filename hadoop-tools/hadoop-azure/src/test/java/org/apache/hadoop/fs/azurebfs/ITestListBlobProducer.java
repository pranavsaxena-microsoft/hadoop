/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.BlobProperty;
import org.apache.hadoop.fs.azurebfs.services.ListBlobConsumer;
import org.apache.hadoop.fs.azurebfs.services.ListBlobProducer;
import org.apache.hadoop.fs.azurebfs.services.ListBlobQueue;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_PRODUCER_QUEUE_MAX_SIZE;

public class ITestListBlobProducer extends AbstractAbfsIntegrationTest {

  public ITestListBlobProducer() throws Exception {
    super();
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue(
        getFileSystem().getAbfsStore().getAbfsConfiguration().getPrefixMode()
            == PrefixMode.BLOB);
  }

  @Test
  public void testProducerWaitingForConsumerLagToGoDown() throws Exception {
    Configuration configuration = Mockito.spy(getRawConfiguration());
    configuration.set(FS_AZURE_PRODUCER_QUEUE_MAX_SIZE, "10");
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(
        configuration);
    AbfsClient client = fs.getAbfsClient();
    AbfsClient spiedClient = Mockito.spy(client);
    fs.getAbfsStore().setClient(spiedClient);
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("/src"));
    ExecutorService executor = Executors.newFixedThreadPool(5);
    List<Future> futureList = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      int iter = i;
      futureList.add(executor.submit(() -> {
        return fs.create(new Path("/src/file" + iter));
      }));
    }
    for(Future future : futureList) {
      future.get();
    }

    AtomicInteger producedBlobs = new AtomicInteger(0);
    AtomicInteger listBlobInvoked = new AtomicInteger(0);

    final ITestListBlobProducer testObj = this;
    final ListBlobQueue queue = new ListBlobQueue(
        fs.getAbfsStore().getAbfsConfiguration().getProducerQueueMaxSize(),
        1);
    final CountDownLatch latch = new CountDownLatch(10);

    Mockito.doAnswer(answer -> {
      synchronized (testObj) {
        listBlobInvoked.incrementAndGet();
        AbfsRestOperation op = client.getListBlobs(answer.getArgument(0),
            answer.getArgument(1), answer.getArgument(2), 1, answer.getArgument(4));
        producedBlobs.incrementAndGet();
        latch.countDown();
        if(producedBlobs.get() > 10) {
          Assert.assertTrue(queue.availableSize() > 0);
        }
        return op;
      }
        })
        .when(spiedClient)
        .getListBlobs(Mockito.nullable(String.class),
            Mockito.nullable(String.class), Mockito.nullable(String.class),
            Mockito.nullable(Integer.class),
            Mockito.nullable(TracingContext.class));


    ListBlobProducer producer = new ListBlobProducer("src/", spiedClient, queue,
        null, Mockito.mock(
        TracingContext.class));
    ListBlobConsumer consumer = new ListBlobConsumer(queue);
    latch.await();

    int oldInvocation = listBlobInvoked.get();
    Assert.assertTrue(listBlobInvoked.get() == oldInvocation);

    while (!consumer.isCompleted()) {
      synchronized (testObj) {
        consumer.consume();
        Assert.assertTrue(queue.availableSize() > 0);
      }
    }

    Assert.assertTrue(producedBlobs.get() == 20);
  }

  @Test
  public void testConsumerWhenProducerThrowException() throws Exception {
    Configuration configuration = Mockito.spy(getRawConfiguration());
    configuration.set(FS_AZURE_PRODUCER_QUEUE_MAX_SIZE, "10");
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

    Mockito.doAnswer(answer -> {
          throw new AbfsRestOperationException(HttpURLConnection.HTTP_CONFLICT, "",
              "", new Exception(""));

        })
        .when(spiedClient)
        .getListBlobs(Mockito.nullable(String.class),
            Mockito.nullable(String.class), Mockito.nullable(String.class),
            Mockito.nullable(Integer.class),
            Mockito.nullable(TracingContext.class));

    ListBlobQueue queue = new ListBlobQueue(getConfiguration().getProducerQueueMaxSize(),
        getConfiguration().getProducerQueueMaxSize());
    ListBlobProducer producer = new ListBlobProducer("src/", spiedClient, queue,
        null, Mockito.mock(
        TracingContext.class));
    ListBlobConsumer consumer = new ListBlobConsumer(queue);

    Boolean exceptionCaught = false;
    try {
      while (!consumer.isCompleted()) {
        consumer.consume();
      }
    } catch (AzureBlobFileSystemException e) {
      exceptionCaught = true;
    }

    Assert.assertTrue(exceptionCaught);
  }
}
