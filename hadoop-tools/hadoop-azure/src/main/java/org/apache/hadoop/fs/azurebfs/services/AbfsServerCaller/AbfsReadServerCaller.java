package org.apache.hadoop.fs.azurebfs.services.AbfsServerCaller;

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import java.net.URL;

public interface AbfsReadServerCaller {
    AbfsRestOperation operate(URL url, Byte[] buffer, String sasToken,
                              ReadRequestParameters readRequestParameters, TracingContext tracingContext);
}
