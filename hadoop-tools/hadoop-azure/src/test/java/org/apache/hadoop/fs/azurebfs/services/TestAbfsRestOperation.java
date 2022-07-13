package org.apache.hadoop.fs.azurebfs.services;

import org.mockito.Mockito;

import java.io.IOException;

public class TestAbfsRestOperation {
    public static void setHttpOperation(AbfsHttpOperation abfsHttpOperation, AbfsRestOperation abfsRestOperation) throws IOException {
        Mockito.doReturn(abfsHttpOperation).when(abfsRestOperation).getHttpOperation(Mockito.any(), Mockito.any(), Mockito.any());
    }

    public static void noAuth(AbfsRestOperation abfsRestOperation) throws IOException {
        Mockito.doNothing().when(abfsRestOperation).authenticate(Mockito.any());
    }

    public static void noSendMetric(AbfsRestOperation abfsRestOperation) {
        Mockito.doNothing().when(abfsRestOperation).sendMetrics(Mockito.any());
    }

}
