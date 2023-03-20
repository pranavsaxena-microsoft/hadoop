package org.apache.hadoop.fs.azurebfs;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class BlobListXmlParser extends DefaultHandler {
  private final BlobList blobList;

  public BlobListXmlParser(final BlobList blobList) {
    this.blobList = blobList;
  }

  @Override
  public void startElement(final String uri,
      final String localName,
      final String qName,
      final Attributes attributes) throws SAXException {
    super.startElement(uri, localName, qName, attributes);
  }

  @Override
  public void endElement(final String uri,
      final String localName,
      final String qName)
      throws SAXException {
    super.endElement(uri, localName, qName);
  }

  @Override
  public void characters(final char[] ch, final int start, final int length)
      throws SAXException {
    super.characters(ch, start, length);
  }
}
