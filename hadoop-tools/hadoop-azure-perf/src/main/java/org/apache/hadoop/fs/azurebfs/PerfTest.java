package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.statistics.IOStatisticsLogging;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IOSTATISTICS_LOGGING_LEVEL_INFO;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_KB;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

public class PerfTest extends PerfTestBase {

    Long TEST_TIME = 60*60*1000l;
    private final String TEST_PATH = "/testfile";

    Logger LOG =
            LoggerFactory.getLogger(PerfTest.class);

    public PerfTest() throws Exception {
        super();
        this.setup();
    }

    public static void main(String[] args) throws Exception {
        PerfTest perfTest = new PerfTest();
        perfTest.perfTest(args[0]);
    }
    public void perfTest(String path) throws Exception {
        MetricHelper.startPlot(path);
        for(int i=0; i<5;i++) {
            new Thread(() -> {
                while(true) {
                    try {
                        testReadWriteAndSeek(40 * ONE_MB, 4* ONE_MB, 1, 8*ONE_KB);
                    } catch (Exception e) {

                    }
                }
            }).start();
        }

        while(true);

    }

    private void testReadWriteAndSeek(int fileSize, int bufferSize, Integer seek1, Integer seek2) throws Exception {
        final AzureBlobFileSystem fs = getFileSystem();
        final AbfsConfiguration abfsConfiguration = fs.getAbfsStore().getAbfsConfiguration();
        abfsConfiguration.setWriteBufferSize(bufferSize);
        abfsConfiguration.setReadBufferSize(bufferSize);

        final byte[] b = new byte[fileSize];
        new Random().nextBytes(b);

        Path testPath = path(TEST_PATH);
        FSDataOutputStream stream = fs.create(testPath);
        try {
            stream.write(b);
        } finally{
            stream.close();
        }
        IOStatisticsLogging.logIOStatisticsAtLevel(LOG, IOSTATISTICS_LOGGING_LEVEL_INFO, stream);

        final byte[] readBuffer = new byte[fileSize];
        int result;
        IOStatisticsSource statisticsSource = null;
        try (FSDataInputStream inputStream = fs.open(testPath)) {
            statisticsSource = inputStream;
            ((AbfsInputStream) inputStream.getWrappedStream()).registerListener(
                    new TracingHeaderValidator(abfsConfiguration.getClientCorrelationId(),
                            fs.getFileSystemId(), FSOperationType.READ, true, 0,
                            ((AbfsInputStream) inputStream.getWrappedStream())
                                    .getStreamID()));
            Long start = new Date().toInstant().toEpochMilli();
            inputStream.read(readBuffer, fileSize - seek1, seek1);

            if(seek2 != 0) {
                inputStream.read(readBuffer, fileSize - seek1 -seek2, seek2);
            }

            MetricHelper.push(new Date().toInstant().toEpochMilli() - start);

//            inputStream.seek(bufferSize);
//            result = inputStream.read(readBuffer, bufferSize, bufferSize);
//            assertNotEquals(-1, result);
//
//            //to test tracingHeader for case with bypassReadAhead == true
//            inputStream.seek(0);
//            byte[] temp = new byte[5];
//            int t = inputStream.read(temp, 0, 1);
//
//            inputStream.seek(0);
//            result = inputStream.read(readBuffer, 0, bufferSize);
        }
        IOStatisticsLogging.logIOStatisticsAtLevel(LOG, IOSTATISTICS_LOGGING_LEVEL_INFO, statisticsSource);
//        assertArrayEquals(readBuffer, b);
    }
}
