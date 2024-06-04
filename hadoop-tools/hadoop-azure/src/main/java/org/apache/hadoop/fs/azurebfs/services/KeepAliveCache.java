package org.apache.hadoop.fs.azurebfs.services;

import java.io.Closeable;
import java.io.IOException;
import java.util.Stack;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.http.HttpClientConnection;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KAC_DEFAULT_CONN_TTL;

public class KeepAliveCache extends Stack<KeepAliveCache.KeepAliveEntry>
    implements
    Closeable {

  private final Timer timer;

  private final TimerTask timerTask;

  private boolean isClosed;

  private static final AtomicInteger KAC_COUNTER = new AtomicInteger(0);

  private final int maxConn;

  private final long connectionIdleTTL;

  private boolean isPaused = false;

  synchronized void pauseThread() {
    isPaused = true;
  }

  synchronized void resumeThread() {
    isPaused = false;
  }

  public long getConnectionIdleTTL() {
    return connectionIdleTTL;
  }

  public KeepAliveCache(AbfsConfiguration abfsConfiguration) {
    this.timer = new Timer(
        String.format("abfs-kac-" + KAC_COUNTER.getAndIncrement()), true);
    String sysPropMaxConn = System.getProperty(HTTP_MAX_CONN_SYS_PROP);
    if (sysPropMaxConn == null) {
      this.maxConn = abfsConfiguration.getMaxApacheHttpClientCacheConnections();
    } else {
      maxConn = Integer.parseInt(sysPropMaxConn);
    }

    this.connectionIdleTTL
        = abfsConfiguration.getMaxApacheHttpClientConnectionIdleTime();
    this.timerTask = new TimerTask() {
      @Override
      public void run() {
        synchronized (KeepAliveCache.this) {
          if (isPaused) {
            return;
          }
          long currentTime = System.currentTimeMillis();
          int i;

          for (i = 0; i < size(); i++) {
            KeepAliveEntry e = elementAt(i);
            if ((currentTime - e.idleStartTime) > connectionIdleTTL
                || e.httpClientConnection.isStale()) {
              HttpClientConnection hc = e.httpClientConnection;
              closeHtpClientConnection(hc);
            } else {
              break;
            }
          }
          subList(0, i).clear();
        }
      }
    };
    timer.schedule(timerTask, 0, connectionIdleTTL);
  }

  private void closeHtpClientConnection(final HttpClientConnection hc) {
    try {
      hc.close();
    } catch (IOException ignored) {

    }
  }

  @Override
  public synchronized void close() {
    isClosed = true;
    timerTask.cancel();
    timer.purge();
    while (!empty()) {
      KeepAliveEntry e = pop();
      closeHtpClientConnection(e.httpClientConnection);
    }
  }

  public synchronized HttpClientConnection get()
      throws IOException {
    if (isClosed) {
      throw new IOException("KeepAliveCache is closed");
    }
    if (empty()) {
      return null;
    }
    HttpClientConnection hc = null;
    long currentTime = System.currentTimeMillis();
    do {
      KeepAliveEntry e = pop();
      if ((currentTime - e.idleStartTime) > connectionIdleTTL
          || e.httpClientConnection.isStale()) {
        e.httpClientConnection.close();
      } else {
        hc = e.httpClientConnection;
      }
    } while ((hc == null) && (!empty()));
    return hc;
  }

  public synchronized void put(HttpClientConnection httpClientConnection) {
    if (isClosed) {
      return;
    }
    if (size() >= maxConn) {
      closeHtpClientConnection(httpClientConnection);
      return;
    }
    KeepAliveEntry entry = new KeepAliveEntry(httpClientConnection,
        System.currentTimeMillis());
    push(entry);
  }

  static class KeepAliveEntry {

    private final HttpClientConnection httpClientConnection;

    private final long idleStartTime;

    KeepAliveEntry(HttpClientConnection hc, long idleStartTime) {
      this.httpClientConnection = hc;
      this.idleStartTime = idleStartTime;
    }

    @Override
    public boolean equals(final Object o) {
      if (o instanceof KeepAliveEntry) {
        return httpClientConnection.equals(
            ((KeepAliveEntry) o).httpClientConnection);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return httpClientConnection.hashCode();
    }
  }
}
