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

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.utils.Listener;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;

/**
 * Used to validate correlation identifiers provided during testing against
 * values that get associated with a request through its TracingContext instance
 */
public class TracingHeaderValidator implements Listener {
  private String clientCorrelationId;
  private String fileSystemId;
  private String primaryRequestId = EMPTY_STRING;
  private boolean needsPrimaryRequestId;
  private String streamID = "";
  private FSOperationType operation;
  private int retryNum;
  private TracingHeaderFormat format;

  private static final String GUID_PATTERN = "^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$";

  @Override
  public void callTracingHeaderValidator(String tracingContextHeader,
      TracingHeaderFormat format) {
    this.format = format;
    validateTracingHeader(tracingContextHeader);
  }

  @Override
  public TracingHeaderValidator getClone() {
    TracingHeaderValidator tracingHeaderValidator = new TracingHeaderValidator(
        clientCorrelationId, fileSystemId, operation, needsPrimaryRequestId,
        retryNum, streamID);
    tracingHeaderValidator.primaryRequestId = primaryRequestId;
    return tracingHeaderValidator;
  }

  public TracingHeaderValidator(String clientCorrelationId, String fileSystemId,
                                FSOperationType operation, boolean needsPrimaryRequestId, int retryNum) {
    this.clientCorrelationId = clientCorrelationId;
    this.fileSystemId = fileSystemId;
    this.operation = operation;
    this.retryNum = retryNum;
    this.needsPrimaryRequestId = needsPrimaryRequestId;
  }

  public TracingHeaderValidator(String clientCorrelationId, String fileSystemId,
                                FSOperationType operation, boolean needsPrimaryRequestId, int retryNum,
                                String streamID) {
    this(clientCorrelationId, fileSystemId, operation, needsPrimaryRequestId,
        retryNum);
    this.streamID = streamID;
  }

  private void validateTracingHeader(String tracingContextHeader) {
    String[] idList = tracingContextHeader.split(":");
    validateBasicFormat(idList);
    if (format != TracingHeaderFormat.ALL_ID_FORMAT) {
      return;
    }
  }

  private void validateBasicFormat(String[] idList) {
  }

  /**
   * Sets the value of expected Hadoop operation
   * @param operation Hadoop operation code (String of two characters)
   */
  @Override
  public void setOperation(FSOperationType operation) {
    this.operation = operation;
  }

  @Override
  public void updatePrimaryRequestID(String primaryRequestId) {
    this.primaryRequestId = primaryRequestId;
  }
}
