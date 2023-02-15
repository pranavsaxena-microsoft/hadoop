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

package org.apache.hadoop.fs.azurebfs.oauth2;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Object representing the AAD access token to use when making HTTP requests to Azure Data Lake Storage.
 */
public class AzureADToken {
  private static final Logger LOG = LoggerFactory.getLogger(AzureADAuthenticator.class);
  private String accessToken;
  private Date expiry;

  public String getAccessToken() {
    if (accessToken.length() == 0) {
      LOG.debug("The token value obtained is empty");
    }
    LOG.debug("The token is " + accessToken.substring(0, Math.min(accessToken.length(), 5)) + "...." +
        accessToken.substring(Math.max(accessToken.length() - 5, 0)));
    return this.accessToken;
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  public Date getExpiry() {
    return new Date(this.expiry.getTime());
  }

  public void setExpiry(Date expiry) {
    this.expiry = new Date(expiry.getTime());
  }

}