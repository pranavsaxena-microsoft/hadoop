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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Hashtable;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test attribute operations.
 */
public class ITestAzureBlobFileSystemAttributes extends AbstractAbfsIntegrationTest {
  private static final EnumSet<XAttrSetFlag> CREATE_FLAG = EnumSet.of(XAttrSetFlag.CREATE);
  private static final EnumSet<XAttrSetFlag> REPLACE_FLAG = EnumSet.of(XAttrSetFlag.REPLACE);

  public ITestAzureBlobFileSystemAttributes() throws Exception {
    super();
  }

  /**
   * Test GetXAttr() and SetXAttr() over blob endpoint as well as dfs endpoint.
   * DFS does not support Unicode characters in user-defined metadata properties.
   * Blob Endpoint supports Unicode encoded in UTF_8 character encoding.
   * @throws Exception
   */
  @Test
  public void testGetSetXAttr() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("a/b");
    fs.create(path);

    String attributeName1 = "user.attribute1";
    String attributeName2 = "user.attribute2";
    String decodedAttributeValue1;
    String decodedAttributeValue2;
    byte[] attributeValue1;
    byte[] attributeValue2;

    if(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB) {
      Assume.assumeTrue(!getIsNamespaceEnabled(fs));
      decodedAttributeValue1 = "hi";
      decodedAttributeValue2 = "Блюз"; //Блюз //你好
      attributeValue1 = decodedAttributeValue1.getBytes(StandardCharsets.UTF_8);
      attributeValue2 = decodedAttributeValue2.getBytes(StandardCharsets.UTF_8);
    }
    else {
      decodedAttributeValue1 = "hi";
      decodedAttributeValue2 = "hello"; // DFS Endpoint only Supports ASCII
      attributeValue1 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue1);
      attributeValue2 = fs.getAbfsStore().encodeAttribute(decodedAttributeValue2);
    }

    // Attribute not present initially
    assertNull(fs.getXAttr(path, attributeName1));
    assertNull(fs.getXAttr(path, attributeName2));

    // Set the Attributes
    fs.setXAttr(path, attributeName1, attributeValue1);

    // Check if the attribute is retrievable
    byte[] rv = fs.getXAttr(path, attributeName1);
    assertTrue(Arrays.equals(rv, attributeValue1));
    assertEquals(new String(rv, StandardCharsets.UTF_8), decodedAttributeValue1);

    // Set the second Attribute
    fs.setXAttr(path, attributeName2, attributeValue2);

    // Check all the attributes present and previous Attribute not overridden
    rv = fs.getXAttr(path, attributeName1);
    assertTrue(Arrays.equals(rv, attributeValue1));
    assertEquals(new String(rv, StandardCharsets.UTF_8), decodedAttributeValue1);
    rv = fs.getXAttr(path, attributeName2);
    assertTrue(Arrays.equals(rv, attributeValue2));
    assertEquals(new String(rv, StandardCharsets.UTF_8), decodedAttributeValue2);
  }

  /**
   * Trying to set same attribute multiple times should result in no failure
   * @throws Exception
   */
  @Test
  public void testSetXAttrMultipleOperations() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("a/b");
    fs.create(path);

    String attributeName1 = "user.attribute1";
    byte[] attributeValue1;

    if(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB) {
      Assume.assumeTrue(!getIsNamespaceEnabled(fs));
      attributeValue1 = "hi".getBytes(StandardCharsets.UTF_8);
    }
    else {
      attributeValue1 = fs.getAbfsStore().encodeAttribute("hi");
    }

    // Attribute not present initially
    assertNull(fs.getXAttr(path, attributeName1));

    // Set the Attributes Multiple times
    fs.setXAttr(path, attributeName1, attributeValue1);
    fs.setXAttr(path, attributeName1, attributeValue1);

    // Check if the attribute is retrievable
    byte[] rv = fs.getXAttr(path, attributeName1);
    assertTrue(Arrays.equals(rv, attributeValue1));
  }

  /**
   * Test that setting metadata over marker blob do not override
   * x-ms-meta-hdi_IsFolder
   * TODO: Confirm Expected Behavior
   * @throws Exception
   */
  @Test
  public void testSetXAttrOverMarkerBlob() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("a/b");
    fs.mkdirs(path);

    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(path, fs));

    String attributeName1 = "user.attribute1";
    byte[] attributeValue1;

    if(fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB) {
      Assume.assumeTrue(!getIsNamespaceEnabled(fs));
      attributeValue1 = "hi".getBytes(StandardCharsets.UTF_8);
    }
    else {
      attributeValue1 = fs.getAbfsStore().encodeAttribute("hi");
    }

    // Attribute not present initially
    assertNull(fs.getXAttr(path, attributeName1));

    // Set the Attribute on marker blob
    fs.setXAttr(path, attributeName1, attributeValue1);

    // Check if the attribute is retrievable
    byte[] rv = fs.getXAttr(path, attributeName1);
    assertTrue(Arrays.equals(rv, attributeValue1));

    // Check if Marker blob still exists as marker.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(path, fs));
  }

  @Test
  public void testSetGetXAttr() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    AbfsConfiguration conf = fs.getAbfsStore().getAbfsConfiguration();
    Assume.assumeTrue(getIsNamespaceEnabled(fs));

    byte[] attributeValue1 = fs.getAbfsStore().encodeAttribute("hi");
    byte[] attributeValue2 = fs.getAbfsStore().encodeAttribute("你好");
    String attributeName1 = "user.asciiAttribute";
    String attributeName2 = "user.unicodeAttribute";
    Path testFile = path("setGetXAttr");

    // after creating a file, the xAttr should not be present
    touch(testFile);
    assertNull(fs.getXAttr(testFile, attributeName1));

    // after setting the xAttr on the file, the value should be retrievable
    fs.registerListener(
        new TracingHeaderValidator(conf.getClientCorrelationId(),
            fs.getFileSystemId(), FSOperationType.SET_ATTR, true, 0));
    fs.setXAttr(testFile, attributeName1, attributeValue1);
    fs.setListenerOperation(FSOperationType.GET_ATTR);
    assertArrayEquals(attributeValue1, fs.getXAttr(testFile, attributeName1));
    fs.registerListener(null);

    // after setting a second xAttr on the file, the first xAttr values should not be overwritten
    fs.setXAttr(testFile, attributeName2, attributeValue2);
    assertArrayEquals(attributeValue1, fs.getXAttr(testFile, attributeName1));
    assertArrayEquals(attributeValue2, fs.getXAttr(testFile, attributeName2));
  }

  @Test
  public void testSetGetXAttrCreateReplace() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(getIsNamespaceEnabled(fs));
    byte[] attributeValue = fs.getAbfsStore().encodeAttribute("one");
    String attributeName = "user.someAttribute";
    Path testFile = path("createReplaceXAttr");

    // after creating a file, it must be possible to create a new xAttr
    touch(testFile);
    fs.setXAttr(testFile, attributeName, attributeValue, CREATE_FLAG);
    assertArrayEquals(attributeValue, fs.getXAttr(testFile, attributeName));

    // however after the xAttr is created, creating it again must fail
    intercept(IOException.class, () -> fs.setXAttr(testFile, attributeName, attributeValue, CREATE_FLAG));
  }

  @Test
  public void testSetGetXAttrReplace() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(getIsNamespaceEnabled(fs));
    byte[] attributeValue1 = fs.getAbfsStore().encodeAttribute("one");
    byte[] attributeValue2 = fs.getAbfsStore().encodeAttribute("two");
    String attributeName = "user.someAttribute";
    Path testFile = path("replaceXAttr");

    // after creating a file, it must not be possible to replace an xAttr
    intercept(IOException.class, () -> {
      touch(testFile);
      fs.setXAttr(testFile, attributeName, attributeValue1, REPLACE_FLAG);
    });

    // however after the xAttr is created, replacing it must succeed
    fs.setXAttr(testFile, attributeName, attributeValue1, CREATE_FLAG);
    fs.setXAttr(testFile, attributeName, attributeValue2, REPLACE_FLAG);
    assertArrayEquals(attributeValue2, fs.getXAttr(testFile, attributeName));
  }
}
