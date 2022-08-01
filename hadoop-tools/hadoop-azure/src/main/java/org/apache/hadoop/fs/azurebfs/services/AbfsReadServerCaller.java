package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import java.net.URL;
import java.util.List;

public interface AbfsReadServerCaller {
    AbfsRestOperation operate(String path,
                              ReadRequestParameters reqParams,
                              URL url,
                              List<AbfsHttpHeader> requestHeaders,
                              byte[] buffer,
                              String sasTokenForReuse,
                              TracingContext tracingContext) throws AzureBlobFileSystemException;
}
