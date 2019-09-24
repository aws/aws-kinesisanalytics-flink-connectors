package com.amazonaws.services.kinesisanalytics.flink.connectors.producer.impl;

import com.amazonaws.services.kinesisanalytics.flink.connectors.exception.RecordCouldNotBeSentException;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.AmazonKinesisFirehoseException;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException;
import com.amazonaws.services.kinesisfirehose.model.ServiceUnavailableException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_MAX_BUFFER_SIZE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_SIZE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.producer.impl.FirehoseProducer.UserRecordResult;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.DEFAULT_DELIVERY_STREAM;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * All tests make relies on best effort to simulate and wait how a multi-threading system should be behave,
 * trying to rely on deterministic results, however, the results and timing depends on the operating system scheduler and JVM.
 * So, if any of these tests failed, you may want to increase the sleep timeout or perhaps comment out the failed ones.
 */
public class FirehoseProducerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FirehoseProducerTest.class);

    @Mock
    private AmazonKinesisFirehose firehoseClient;

    private FirehoseProducer<UserRecordResult, Record> firehoseProducer;

    @BeforeMethod
    public void init() {
        MockitoAnnotations.initMocks(this);

        Properties config = new Properties();
        config.setProperty(FIREHOSE_PRODUCER_BUFFER_MAX_TIMEOUT, "1000");

        this.firehoseProducer = new FirehoseProducer<>(DEFAULT_DELIVERY_STREAM, firehoseClient,
                config);
    }

    @AfterMethod
    public void tearDown() throws  Exception{
        this.firehoseProducer.destroy();
        this.firehoseProducer = null;
    }

    @Test
    public void testFirehoseProducerSingleThreadHappyCase() throws Exception {
        PutRecordBatchResult successResult = new PutRecordBatchResult();
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);

        for (int i = 0; i < DEFAULT_MAX_BUFFER_SIZE; ++i) {
            addRecord(firehoseProducer);
        }
        Thread.sleep(2000);
        LOGGER.debug("Number of outstanding records: {}", firehoseProducer.getOutstandingRecordsCount());
        assertTrue(firehoseProducer.getOutstandingRecordsCount() == 0);
    }

    @Test
    public void testFirehoseProducerMultiThreadHappyCase() throws Exception {
        PutRecordBatchResult successResult = new PutRecordBatchResult();
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<Callable<ListenableFuture<UserRecordResult>>> futures = new ArrayList<>();

        for (int j = 0; j < DEFAULT_MAX_BUFFER_SIZE; ++j) {
            futures.add(() -> addRecord(firehoseProducer));
        }

        exec.invokeAll(futures);
        Thread.currentThread().join(3000);
        LOGGER.debug("Number of outstanding items: {}", firehoseProducer.getOutstandingRecordsCount());
        assertTrue(firehoseProducer.getOutstandingRecordsCount() == 0);
    }

    @Test
    public void testFirehoseProducerMultiThreadFlushSyncHappyCase() throws Exception {
        PutRecordBatchResult successResult = mock(PutRecordBatchResult.class);
        ArgumentCaptor<PutRecordBatchRequest> captor = ArgumentCaptor.forClass(PutRecordBatchRequest.class);

        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<Callable<ListenableFuture<UserRecordResult>>> futures = new ArrayList<>();

        for (int j = 0; j < 400; ++j) {
            futures.add(() -> addRecord(firehoseProducer));
        }

        List<Future<ListenableFuture<UserRecordResult>>> results = exec.invokeAll(futures);

        for (Future f : results) {
            while(!f.isDone()) {
                Thread.sleep(100);
            }
            SettableFuture fi = (SettableFuture) f.get();
            UserRecordResult r = (UserRecordResult) fi.get();
            assertTrue(r.isSuccessful());
        }
        firehoseProducer.flushSync();

        LOGGER.debug("Number of outstanding items: {}", firehoseProducer.getOutstandingRecordsCount());
        verify(firehoseClient).putRecordBatch(captor.capture());
        assertEquals(firehoseProducer.getOutstandingRecordsCount() , 0);
        assertFalse(firehoseProducer.isFlushFailed());
    }


    @Test
    public void testFirehoseProducerMultiThreadFlushAndWaitHappyCase() throws Exception {
        PutRecordBatchResult successResult = mock(PutRecordBatchResult.class);
        ArgumentCaptor<PutRecordBatchRequest> captor = ArgumentCaptor.forClass(PutRecordBatchRequest.class);

        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<Callable<ListenableFuture<UserRecordResult>>> futures = new ArrayList<>();

        for (int j = 0; j < 400; ++j) {
            futures.add(() -> addRecord(firehoseProducer));
        }

        List<Future<ListenableFuture<UserRecordResult>>> results = exec.invokeAll(futures);

        for (Future f : results) {
            while(!f.isDone()) {
                Thread.sleep(100);
            }
            SettableFuture fi = (SettableFuture) f.get();
            UserRecordResult r = (UserRecordResult) fi.get();
            assertTrue(r.isSuccessful());
        }

        while (firehoseProducer.getOutstandingRecordsCount() > 0 && !firehoseProducer.isFlushFailed()) {
            firehoseProducer.flush();
            try {
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                fail();
            }
        }

        LOGGER.debug("Number of outstanding items: {}", firehoseProducer.getOutstandingRecordsCount());
        verify(firehoseClient).putRecordBatch(captor.capture());
        assertEquals(firehoseProducer.getOutstandingRecordsCount(), 0);
        assertFalse(firehoseProducer.isFlushFailed());
    }

    @Test
    public void testFirehoseProducerSingleThreadTimeoutExpiredHappyCase() throws Exception {
        PutRecordBatchResult successResult = new PutRecordBatchResult();
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);

        for (int i = 0; i < 100; ++i) {
            addRecord(firehoseProducer);
        }
        Thread.sleep(2000);
        assertTrue(firehoseProducer.getOutstandingRecordsCount() == 0);
    }

    @Test
    public void testFirehoseProducerSingleThreadBufferIsFullHappyCase() throws Exception {
        PutRecordBatchResult successResult = new PutRecordBatchResult();
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);

        for (int i = 0; i < 2 * DEFAULT_MAX_BUFFER_SIZE; ++i) {
            addRecord(firehoseProducer);
        }

        Thread.sleep(2000);
        assertTrue(firehoseProducer.getOutstandingRecordsCount() == 0);
    }

    /**
     * This test is responsible for checking if the consumer thread has performed the work or not, so there is no way to
     * throw an exception to be catch here, so the assertion goes along the fact if the buffer was flushed or not.
     * @throws Exception
     */
    @Test
    public void testFirehoseProducerSingleThreadFailedToSendRecords() throws Exception {
        ResourceNotFoundException ex = mock(ResourceNotFoundException.class);
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenThrow(ex);

        for (int i = 0; i < DEFAULT_MAX_BUFFER_SIZE; ++i) {
            addRecord(firehoseProducer);
        }
        Thread.sleep(2000);
        assertTrue(firehoseProducer.isFlushFailed());
    }

    /**
     * This test is responsible for checking if the consumer thread has performed the work or not. So if there is a
     * retryable exception, it should keep flushing the buffer until it's empty.
     */
    @Test
    public void testFirehoseProducerKeepSendingRecords() throws Exception {
        PutRecordBatchResult successResult = new PutRecordBatchResult();
        ServiceUnavailableException ex = mock(ServiceUnavailableException.class);
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class)))
                .thenThrow(ex)
                .thenReturn(successResult);

        for (int i = 0; i < DEFAULT_MAX_BUFFER_SIZE; ++i) {
            addRecord(firehoseProducer);
        }
        Thread.sleep(2000);
        assertEquals(firehoseProducer.getOutstandingRecordsCount(), 0);
        assertTrue(!firehoseProducer.isFlushFailed());
    }

    /**
     * This test is responsible for checking if it returns the failure records for the retry.
     **/
    @Test
    public void testFirehoseProducerSubmitRecordWithFailure()  {
        PutRecordBatchResult failedResult = new PutRecordBatchResult()
                .withFailedPutCount(1)
                .withRequestResponses(new PutRecordBatchResponseEntry()
                        .withErrorCode("500")
                        .withErrorMessage("Service Unavailable"));
        PutRecordBatchResult successResult = new PutRecordBatchResult();
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class)))
                .thenReturn(failedResult)
                .thenReturn(successResult);
        Queue<Record> records = new ArrayDeque<>();
        for (int i = 0; i < DEFAULT_MAX_BUFFER_SIZE; ++i) {
            records.offer(new Record().withData(ByteBuffer.wrap(
                    RandomStringUtils.randomAlphabetic(64).getBytes())));
        }

        ArgumentCaptor<Queue> argument = ArgumentCaptor.forClass(Queue.class);
        FirehoseProducer<UserRecordResult, Record> producer = spy(firehoseProducer);
        producer.submitBatchWithRetry(records);
        verify(producer, times(2)).submitBatch(argument.capture());
        List<Queue> queues = argument.getAllValues();
        assertEquals(queues.get(0).size(), DEFAULT_MAX_BUFFER_SIZE);
        assertEquals(queues.get(1).size(), 1);
    }

    /**
     * Test there is failed submitted records. Those records should be flushed first when a new record is added.
     * @throws Exception
     */
    @Test
    public void testFlushingBufferWithFixedRetry() throws  Exception {
        firehoseProducer.destroy();
        FirehoseProducer<UserRecordResult, Record> producer =
                spy(new FirehoseProducer<>(DEFAULT_DELIVERY_STREAM, firehoseClient,
                        +                        5, 0, 3, 0,
                        +                        0, 0, 0, 0));
        producer.flusher.submit(() -> producer.flushBuffer());

        PutRecordBatchResult successResult = new PutRecordBatchResult();
        PutRecordBatchResult failedResult = new PutRecordBatchResult()
                .withFailedPutCount(1)
                .withRequestResponses(new PutRecordBatchResponseEntry()
                        .withErrorCode("500")
                        .withErrorMessage("Service Unavailable"));
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class)))
                .thenReturn(failedResult);

        for (int i = 0; i < 5; i++) {
            addRecord(producer);
        }
        Thread.sleep(2000);
        assertTrue(!producer.isFlushFailed());
        assertEquals(producer.getOutstandingRecordsCount(), 1);

        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);
        ArgumentCaptor<Queue> argument = ArgumentCaptor.forClass(Queue.class);
        for (int i = 0; i < 5; i++) {
            addRecord(producer);
        }
        Thread.sleep(2000);
        verify(producer,times(3)).submitBatchWithRetry(argument.capture());
        List<Queue> queues = argument.getAllValues();
        assertEquals(queues.get(0).size(), 5);
        assertEquals(queues.get(1).size(), 1);
        assertEquals(queues.get(2).size(), 5);
        assertTrue(!producer.isFlushFailed());
        assertEquals(producer.getOutstandingRecordsCount(), 0);
        producer.destroy();
    }

    /**
     * Test if addUserRecord throws an exception , when get NON-Retryable exception while flushing,
     * @throws Exception
     */
    @Test
    public void testAddRecordAfterFlushBufferFailed() throws Exception {
        firehoseProducer.destroy();
        AmazonKinesisFirehoseException ex = new AmazonKinesisFirehoseException("Non retryable exception");
        ex.setStatusCode(400);
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenThrow(ex);

        Properties config = new Properties();
        config.setProperty(FIREHOSE_PRODUCER_BUFFER_MAX_SIZE, "5");
        FirehoseProducer<UserRecordResult, Record> producer =
                new FirehoseProducer<>(DEFAULT_DELIVERY_STREAM, firehoseClient, config);

        for (int i = 0; i < 5; ++i) {
            addRecord(producer);
        }
        Thread.sleep(1000);
        assertTrue(producer.isFlushFailed());
        try {
            for (int i = 0; i < 5; ++i) {
                addRecord(producer);
            }
        } catch (AmazonKinesisFirehoseException e) {
            assertEquals(e.getStatusCode(), 400);
            assertTrue(producer.isFlushFailed());
        }
        Thread.sleep(2000);
        producer.destroy();
    }

    private ListenableFuture<UserRecordResult> addRecord(final FirehoseProducer producer) throws Exception {
        try {
            Record record = new Record().withData(ByteBuffer.wrap(
                    RandomStringUtils.randomAlphabetic(64).getBytes()));
            return producer.addUserRecord(record);
        } catch (Exception e) {
            if (e instanceof  AmazonKinesisFirehoseException) {
                throw e;
            } else if (e instanceof RecordCouldNotBeSentException) {
                throw e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }
}
