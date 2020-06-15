/*
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

package com.amazonaws.services.kinesisanalytics.flink.connectors.producer.impl;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_MAX_BUFFER_SIZE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.producer.impl.FirehoseProducer.UserRecordResult;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.DEFAULT_DELIVERY_STREAM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
        PutRecordBatchResult failedResult = new PutRecordBatchResult()
                .withFailedPutCount(1)
                .withRequestResponses(new PutRecordBatchResponseEntry()
                        .withErrorCode("400")
                        .withErrorMessage("Invalid Schema"));
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(failedResult);

        for (int i = 0; i < DEFAULT_MAX_BUFFER_SIZE; ++i) {
            addRecord(firehoseProducer);
        }
        Thread.sleep(2000);
        assertEquals(firehoseProducer.getOutstandingRecordsCount(), DEFAULT_MAX_BUFFER_SIZE);
        assertTrue(firehoseProducer.isFlushFailed());
    }

    private ListenableFuture<UserRecordResult> addRecord(final FirehoseProducer producer) {
        try {
            Record record = new Record().withData(ByteBuffer.wrap(
                    RandomStringUtils.randomAlphabetic(64).getBytes()));
            return producer.addUserRecord(record);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
