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

import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


public class BlobErrorMessageXmlParser extends DefaultHandler {

  private final static String codeXmlElement = "Code";

  private final static String messageXmlElement = "Message";

  /**
   * Maintains the value in a given XML-element.
   */
  private StringBuilder bld = new StringBuilder();

  /**
   * Maintains the stack of XML-elements in memory at a given moment.
   */
  private final Stack<String> elements = new Stack<>();

  private String code;

  private String message;


  @Override
  public void startElement(final String uri,
      final String localName,
      final String qName,
      final Attributes attributes) throws SAXException {
    elements.push(localName);
  }

  @Override
  public void endElement(final String uri,
      final String localName,
      final String qName)
      throws SAXException {
    String currentNode = elements.pop();
    switch (currentNode) {
    case codeXmlElement:
      code = bld.toString();
    case messageXmlElement:
      message = bld.toString();
    }
    bld = new StringBuilder();
  }

  @Override
  public void characters(final char[] ch, final int start, final int length)
      throws SAXException {
    bld.append(ch, start, length);
  }

  public String getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }
}
