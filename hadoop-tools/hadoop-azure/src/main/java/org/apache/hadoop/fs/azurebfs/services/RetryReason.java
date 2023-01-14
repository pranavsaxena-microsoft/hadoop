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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public enum RetryReason {

  CONNECTION_TIMEOUT(
      new RetryReasonMechanisms() {
        @Override
        public boolean canCapture(final Exception exceptionCaptured,
            final Integer statusCode) {
          return exceptionCaptured != null
              && "connect timed out".equalsIgnoreCase(
              exceptionCaptured.getMessage());
        }

        @Override
        public String getAbbreviation(final Exception ex,
            final Integer statusCode,
            final String serverErrorMessage) {
          return "CT";
        }
      }, 2),
  READ_TIMEOUT(
      new RetryReasonMechanisms() {
        @Override
        public boolean canCapture(final Exception exceptionCaptured,
            final Integer statusCode) {
          return exceptionCaptured != null && "Read timed out".equalsIgnoreCase(
              exceptionCaptured.getMessage());
        }

        @Override
        public String getAbbreviation(final Exception ex,
            final Integer statusCode,
            final String serverErrorMessage) {
          return "RT";
        }
      }, 2),
  UNKNOWN_HOST(new RetryReasonMechanisms() {
    @Override
    public boolean canCapture(final Exception exceptionCaptured,
        final Integer statusCode) {
      return exceptionCaptured instanceof UnknownHostException;
    }

    @Override
    public String getAbbreviation(final Exception ex,
        final Integer statusCode,
        final String serverErrorMessage) {
      return "UH";
    }
  }, 2),
  CONNECTION_RESET(
      new RetryReasonMechanisms() {
        @Override
        public boolean canCapture(final Exception exceptionCaptured,
            final Integer statusCode) {
          return exceptionCaptured != null
              && exceptionCaptured.getMessage() != null
              && exceptionCaptured.getMessage().contains("Connection reset");
        }

        @Override
        public String getAbbreviation(final Exception ex,
            final Integer statusCode,
            final String serverErrorMessage) {
          return "CR";
        }
      }, 2),
  STATUS_5XX(
      new RetryReasonMechanisms() {
        @Override
        public boolean canCapture(final Exception exceptionCaptured,
            final Integer statusCode) {
          return statusCode / 100 == 5;
        }

        @Override
        public String getAbbreviation(final Exception ex,
            final Integer statusCode,
            String serverErrorMessage) {
          if (statusCode == 503) {
            //ref: https://github.com/apache/hadoop/pull/4564/files#diff-75a2f54df6618d4015c63812e6a9916ddfb475d246850edfd2a6f57e36805e79
            serverErrorMessage = serverErrorMessage.split(
                System.lineSeparator(),
                2)[0];
            if ("Ingress is over the account limit.".equalsIgnoreCase(
                serverErrorMessage)) {
              return "ING";
            }
            if ("Egress is over the account limit.".equalsIgnoreCase(
                serverErrorMessage)) {
              return "EGR";
            }
            if ("Operations per second is over the account limit.".equalsIgnoreCase(
                serverErrorMessage)) {
              return "OPR";
            }
            return "503";
          }
          return statusCode + "";
        }
      }, 0),
  STATUS_4XX(
      new RetryReasonMechanisms() {
        @Override
        public boolean canCapture(final Exception exceptionCaptured,
            final Integer statusCode) {
          return statusCode / 100 == 4;
        }

        @Override
        public String getAbbreviation(final Exception ex,
            final Integer statusCode,
            final String serverErrorMessage) {
          return statusCode + "";
        }
      }, 0),
  UNKNOWN_SOCKET_EXCEPTION(
      new RetryReasonMechanisms() {
        @Override
        public boolean canCapture(final Exception exceptionCaptured,
            final Integer statusCode) {
          return exceptionCaptured instanceof SocketException;
        }

        @Override
        public String getAbbreviation(final Exception ex,
            final Integer statusCode,
            final String serverErrorMessage) {
          return "SE";
        }
      }, 1),
  UNKNOWN_IO_EXCEPTION(
      new RetryReasonMechanisms() {
        @Override
        public boolean canCapture(final Exception exceptionCaptured,
            final Integer statusCode) {
          return exceptionCaptured instanceof IOException;
        }

        @Override
        public String getAbbreviation(final Exception ex,
            final Integer statusCode,
            final String serverErrorMessage) {
          return "IOE";
        }
      }, 0);

  private RetryReasonMechanisms mechanism = null;

  private int rank = 0;

  RetryReason(RetryReasonMechanisms mechanism,
      int rank) {
    this.mechanism = mechanism;
    this.rank = rank;
  }

  public String getAbbreviation(Exception ex,
      Integer statusCode,
      String serverErrorMessage) {
    return mechanism.getAbbreviation(ex, statusCode, serverErrorMessage);
  }

  private static List<RetryReason> retryReasonLSortedist;

  private synchronized static void sortRetryReason() {
    if (retryReasonLSortedist != null) {
      return;
    }
    List<RetryReason> list = new ArrayList<>();
    for (RetryReason reason : values()) {
      list.add(reason);
    }
    list.sort((c1, c2) -> {
      return c1.rank - c2.rank;
    });
    retryReasonLSortedist = list;
  }

  static RetryReason getEnum(Exception ex, Integer statusCode) {
    RetryReason retryReasonResult = null;
    if (retryReasonLSortedist == null) {
      sortRetryReason();
    }
    for (RetryReason retryReason : retryReasonLSortedist) {
      if (retryReason.mechanism != null) {
        if (retryReason.mechanism.canCapture(ex, statusCode)) {
          retryReasonResult = retryReason;
        }
      }
    }
    return retryReasonResult;
  }
}
