package org.apache.hadoop.fs.azurebfs.services;

import com.azure.storage.fastpath.exceptions.FastpathRequestException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsFastpathException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.ReadRequestParameters;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import java.net.URL;
import java.util.List;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_GET;

public class AbfsReadFastpathServerCaller implements AbfsReadServerCaller {

    private FastpathAbfsInputStreamHelper fastpathAbfsInputStreamHelper;
    private AbfsClient abfsClient;

    public AbfsReadFastpathServerCaller(FastpathAbfsInputStreamHelper fastpathAbfsInputStreamHelper, AbfsClient abfsClient) {
        this.fastpathAbfsInputStreamHelper = fastpathAbfsInputStreamHelper;
        this.abfsClient = abfsClient;
    }

    @Override
    public AbfsRestOperation operate(String path, ReadRequestParameters reqParams, URL url, List<AbfsHttpHeader> requestHeaders,
                                     byte[] buffer, String sasTokenForReuse, TracingContext tracingContext)
            throws AzureBlobFileSystemException {

        final AbfsFastpathSessionInfo abfsFastpathSessionInfo = fastpathAbfsInputStreamHelper.getAbfsFastpathSessionInfo();
        final AbfsRestOperation op = new AbfsRestOperation(
                AbfsRestOperationType.FastpathRead,
                abfsClient,
                HTTP_METHOD_GET,
                url,
                requestHeaders,
                buffer,
                reqParams.getBufferOffset(),
                reqParams.getReadLength(),
                abfsFastpathSessionInfo);

        try {
            op.execute(tracingContext);
            return op;
        } catch (AbfsFastpathException ex) {
            // Fastpath threw irrecoverable exception
            // when FastpathRequestException is received, the request needs to
            // be retried on REST
            if (ex.getCause() instanceof FastpathRequestException) {
                tracingContext.setConnectionMode(
                        AbfsConnectionMode.REST_ON_FASTPATH_REQ_FAILURE);
                abfsFastpathSessionInfo.setConnectionMode(
                                AbfsConnectionMode.REST_ON_FASTPATH_REQ_FAILURE);
            } else {
                // when FastpathConnectionException is received, the request needs to
                // be retried on REST as well as switch AbfsInputStream to REST for
                // all future reads in its lifetime
                tracingContext.setConnectionMode(
                        AbfsConnectionMode.REST_ON_FASTPATH_CONN_FAILURE);
                abfsFastpathSessionInfo.setConnectionMode(
                                AbfsConnectionMode.REST_ON_FASTPATH_CONN_FAILURE);
                fastpathAbfsInputStreamHelper.changeFastPathUseFlag(false);
            }
            return null;
        }
    }
}
