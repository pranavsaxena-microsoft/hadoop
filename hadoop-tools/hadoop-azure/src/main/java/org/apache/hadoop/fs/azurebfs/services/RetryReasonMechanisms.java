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

/**
 * Define the methods that have to be implemented by enums of {@link RetryReason}.
 * */
interface RetryReasonMechanisms {
  /**
   * Implementation defines if the given {@link RetryReason} enum can be mapped to the
   * server response
   * @param exceptionCaptured exception thrown while communicating with server.
   * @param statusCode statusCode returned in the server response.
   * @return if the given enum implementing can be mapped to the server-response.
   * */
  boolean canCapture(Exception exceptionCaptured, Integer statusCode);

  /**
   *
   * */
  String getAbbreviation(Exception ex, Integer statusCode, String serverErrorMessage);
}
