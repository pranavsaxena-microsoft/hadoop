package org.apache.hadoop.fs.azurebfs.services.AbfsServerCaller;

import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.FastpathAbfsInputStreamHelper;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import java.net.URL;

public class AbfsReadFastpathServerCaller implements AbfsReadServerCaller {

    private FastpathAbfsInputStreamHelper fastpathAbfsInputStreamHelper;

    public AbfsReadFastpathServerCaller(FastpathAbfsInputStreamHelper fastpathAbfsInputStreamHelper) {
        this.fastpathAbfsInputStreamHelper = fastpathAbfsInputStreamHelper;
    }

    @Override
    public AbfsRestOperation operate(URL url, Byte[] buffer, String sasToken, ReadRequestParameters readRequestParameters,
                                     TracingContext tracingContext) {
        return null;
    }
}
