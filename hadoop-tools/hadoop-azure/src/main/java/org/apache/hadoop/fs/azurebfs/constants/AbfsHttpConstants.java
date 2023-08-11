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

package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.VersionInfo;

/**
 * Responsible to keep all constant keys used in abfs rest client here.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class AbfsHttpConstants {
  // Abfs Http client constants
  public static final String FILESYSTEM = "filesystem";
  public static final String FILE = "file";
  public static final String DIRECTORY = "directory";
  public static final String APPEND_ACTION = "append";
  public static final String BLOCK = "block";
  public static final String BLOCKLIST = "blocklist";

  public static final String COMP_LIST = "comp=list";
  public static final String COMMITTED = "committed";
  public static final String UNCOMMITTED = "uncommitted";
  public static final String FLUSH_ACTION = "flush";
  public static final String SET_PROPERTIES_ACTION = "setProperties";
  public static final String SET_ACCESS_CONTROL = "setAccessControl";
  public static final String GET_ACCESS_CONTROL = "getAccessControl";
  public static final String CHECK_ACCESS = "checkAccess";
  public static final String GET_STATUS = "getStatus";
  public static final String ACQUIRE_LEASE_ACTION = "acquire";
  public static final String BREAK_LEASE_ACTION = "break";
  public static final String RELEASE_LEASE_ACTION = "release";
  public static final String RENEW_LEASE_ACTION = "renew";
  public static final String DEFAULT_LEASE_BREAK_PERIOD = "0";
  public static final String DEFAULT_TIMEOUT = "90";
  public static final String APPEND_BLOB_TYPE = "appendblob";
  public static final String BLOCK_BLOB_TYPE = "BlockBlob";
  public static final String TOKEN_VERSION = "2";
  public static final String COMMITTED_BLOCKS = "CommittedBlocks";
  public static final String BLOCK_NAME = "Block";
  public static final String NAME = "Name";

  public static final String JAVA_VENDOR = "java.vendor";
  public static final String JAVA_VERSION = "java.version";
  public static final String OS_NAME = "os.name";
  public static final String OS_VERSION = "os.version";
  public static final String OS_ARCH = "os.arch";

  public static final String APN_VERSION = "APN/1.0";
  public static final String CLIENT_VERSION = "Azure Blob FS/" + VersionInfo.getVersion();

  // Abfs Http Verb
  public static final String HTTP_METHOD_DELETE = "DELETE";
  public static final String HTTP_METHOD_GET = "GET";
  public static final String HTTP_METHOD_HEAD = "HEAD";
  public static final String HTTP_METHOD_PATCH = "PATCH";
  public static final String HTTP_METHOD_POST = "POST";
  public static final String HTTP_METHOD_PUT = "PUT";
  /**
   * All status codes less than http 100 signify error
   * and should qualify for retry.
   */
  public static final int HTTP_CONTINUE = 100;

  // Abfs generic constants
  public static final String SINGLE_WHITE_SPACE = " ";
  public static final String EMPTY_STRING = "";
  public static final String FORWARD_SLASH = "/";
  public static final String DOT = ".";
  public static final String PLUS = "+";
  public static final String STAR = "*";
  public static final String COMMA = ",";
  public static final String COLON = ":";
  public static final String EQUAL = "=";
  public static final String QUESTION_MARK = "?";
  public static final String AND_MARK = "&";
  public static final String SEMICOLON = ";";
  public static final String AT = "@";
  public static final String HTTP_HEADER_PREFIX = "x-ms-";
  public static final String HASH = "#";
  public static final String TRUE = "true";
  public static final String ZERO = "0";

  public static final String PLUS_ENCODE = "%20";
  public static final String FORWARD_SLASH_ENCODE = "%2F";
  public static final String AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER = "@";
  public static final String UTF_8 = "utf-8";
  public static final String GMT_TIMEZONE = "GMT";
  public static final String APPLICATION_JSON = "application/json";
  public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
  public static final String APPLICATION_XML = "application/xml";
  public static final String PUT_BLOCK_LIST = "PutBlockList";
  public static final String XML_VERSION = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
  public static final String BLOCK_LIST_START_TAG = "<BlockList>\n";
  public static final String BLOCK_LIST_END_TAG = "</BlockList>\n";
  public static final String LATEST_BLOCK_FORMAT = "<Latest>%s</Latest>\n";

  public static final String ROOT_PATH = "/";
  public static final String ACCESS_MASK = "mask:";
  public static final String ACCESS_USER = "user:";
  public static final String ACCESS_GROUP = "group:";
  public static final String ACCESS_OTHER = "other:";
  public static final String DEFAULT_MASK = "default:mask:";
  public static final String DEFAULT_USER = "default:user:";
  public static final String DEFAULT_GROUP = "default:group:";
  public static final String DEFAULT_OTHER = "default:other:";
  public static final String DEFAULT_SCOPE = "default:";
  public static final String PERMISSION_FORMAT = "%04d";
  public static final String SUPER_USER = "$superuser";
  // The HTTP 100 Continue informational status response code indicates that everything so far
  // is OK and that the client should continue with the request or ignore it if it is already finished.
  public static final String HUNDRED_CONTINUE = "100-continue";

  public static final char CHAR_FORWARD_SLASH = '/';
  public static final char CHAR_EXCLAMATION_POINT = '!';
  public static final char CHAR_UNDERSCORE = '_';
  public static final char CHAR_HYPHEN = '-';
  public static final char CHAR_EQUALS = '=';
  public static final char CHAR_STAR = '*';
  public static final char CHAR_PLUS = '+';
  /**
   * Value that differentiates categories of the http_status.
   * <pre>
   * 100 - 199 : Informational responses
   * 200 - 299 : Successful responses
   * 300 - 399 : Redirection messages
   * 400 - 499 : Client error responses
   * 500 - 599 : Server error responses
   * </pre>
   */
  public static final Integer HTTP_STATUS_CATEGORY_QUOTIENT = 100;

  public static final String BLOB = "Blob";
  private static final String PREFIX = "Prefix";
  public static final String NEXT_MARKER = "NextMarker";
  public static final String METADATA = "Metadata";
  public static final String PROPERTIES = "Properties";
  public static final String BLOB_PREFIX = "BlobPrefix";
  public static final String CONTENT_LEN = "Content-Length";
  public static final String RESOURCE_TYPE = "ResourceType";
  public static final String INVALID_XML = "Invalid XML";
  public static final String COPY_STATUS_SUCCESS = "success";
  public static final String COPY_STATUS_PENDING = "pending";
  public static final String COPY_STATUS_ABORTED = "aborted";
  public static final String COPY_STATUS_FAILED = "failed";
  public static final String HDI_ISFOLDER = "hdi_isfolder";
  public static final Integer BLOB_LEASE_ONE_MINUTE_DURATION = 60;
  public static final String ETAG = "Etag";
  public static final String LAST_MODIFIED_TIME = "Last-Modified";
  public static final String CREATION_TIME   = "Creation-Time";
  public static final String OWNER = "Owner";
  public static final String GROUP = "Group";
  public static final String PERMISSIONS = "Permissions";
  public static final String ACL = "Acl";
  public static final String COPY_ID = "CopyId";
  public static final String COPY_STATUS = "CopyStatus";
  public static final String COPY_SOURCE = "CopySource";
  public static final String COPY_PROGRESS = "CopyProgress";
  public static final String COPY_COMPLETION_TIME = "CopyCompletionTime";
  public static final String COPY_STATUS_DESCRIPTION = "CopyStatusDescription";
  public static final String BLOB_ERROR_CODE_START_XML = "<Code>";
  public static final String BLOB_ERROR_CODE_END_XML = "</Code>";
  public static final String BLOB_ERROR_MESSAGE_START_XML = "<Message>";
  public static final String BLOB_ERROR_MESSAGE_END_XML = "</Message>";

  private AbfsHttpConstants() {}
}
