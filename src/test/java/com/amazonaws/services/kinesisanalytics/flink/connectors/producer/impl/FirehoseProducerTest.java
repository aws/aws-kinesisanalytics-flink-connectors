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

import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.impl.FirehoseProducer.FirehoseThreadFactory;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import org.apache.commons.lang3.RandomStringUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_REGION;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_MAX_BUFFER_SIZE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_BATCH_BYTES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.producer.impl.FirehoseProducer.UserRecordResult;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.DEFAULT_DELIVERY_STREAM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

/**
 * All tests make relies on best effort to simulate and wait how a multi-threading system should be behave,
 * trying to rely on deterministic results, however, the results and timing depends on the operating system scheduler and JVM.
 * So, if any of these tests failed, you may want to increase the sleep timeout or perhaps comment out the failed ones.
 */
public class FirehoseProducerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FirehoseProducerTest.class);

    private static final int KB_512 = 512 * 1_024;

    @Mock
    private AmazonKinesisFirehose firehoseClient;

    private FirehoseProducer<UserRecordResult, Record> firehoseProducer;

    @Captor
    private ArgumentCaptor<PutRecordBatchRequest> putRecordCaptor;

    @BeforeMethod
    public void init() {
        MockitoAnnotations.initMocks(this);

        this.firehoseProducer = createFirehoseProducer();
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
        assertThat(firehoseProducer.getOutstandingRecordsCount()).isEqualTo(0);
    }

    @Test
    public void testFirehoseProducerMultiThreadHappyCase() throws Exception {
        PutRecordBatchResult successResult = new PutRecordBatchResult();
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<Callable<CompletableFuture<UserRecordResult>>> futures = new ArrayList<>();

        for (int j = 0; j < DEFAULT_MAX_BUFFER_SIZE; ++j) {
            futures.add(() -> addRecord(firehoseProducer));
        }

        exec.invokeAll(futures);
        Thread.currentThread().join(3000);
        LOGGER.debug("Number of outstanding items: {}", firehoseProducer.getOutstandingRecordsCount());
        assertThat(firehoseProducer.getOutstandingRecordsCount()).isEqualTo(0);
    }

    @Test
    public void testFirehoseProducerMultiThreadFlushSyncHappyCase() throws Exception {
        PutRecordBatchResult successResult = mock(PutRecordBatchResult.class);
        ArgumentCaptor<PutRecordBatchRequest> captor = ArgumentCaptor.forClass(PutRecordBatchRequest.class);

        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<Callable<CompletableFuture<UserRecordResult>>> futures = new ArrayList<>();

        for (int j = 0; j < 400; ++j) {
            futures.add(() -> addRecord(firehoseProducer));
        }

        List<Future<CompletableFuture<UserRecordResult>>> results = exec.invokeAll(futures);

        for (Future<CompletableFuture<UserRecordResult>> f : results) {
            while(!f.isDone()) {
                Thread.sleep(100);
            }
            CompletableFuture<UserRecordResult> fi = f.get();
            UserRecordResult r = fi.get();
            assertThat(r.isSuccessful()).isTrue();
        }
        firehoseProducer.flushSync();

        LOGGER.debug("Number of outstanding items: {}", firehoseProducer.getOutstandingRecordsCount());
        verify(firehoseClient).putRecordBatch(captor.capture());
        assertThat(firehoseProducer.getOutstandingRecordsCount()).isEqualTo(0);
        assertThat(firehoseProducer.isFlushFailed()).isFalse();
    }


    @Test
    public void testFirehoseProducerMultiThreadFlushAndWaitHappyCase() throws Exception {
        PutRecordBatchResult successResult = mock(PutRecordBatchResult.class);
        ArgumentCaptor<PutRecordBatchRequest> captor = ArgumentCaptor.forClass(PutRecordBatchRequest.class);

        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);

        ExecutorService exec = Executors.newFixedThreadPool(4);
        List<Callable<CompletableFuture<UserRecordResult>>> futures = new ArrayList<>();

        for (int j = 0; j < 400; ++j) {
            futures.add(() -> addRecord(firehoseProducer));
        }

        List<Future<CompletableFuture<UserRecordResult>>> results = exec.invokeAll(futures);

        for (Future<CompletableFuture<UserRecordResult>> f : results) {
            while(!f.isDone()) {
                Thread.sleep(100);
            }
            CompletableFuture<UserRecordResult> fi = f.get();
            UserRecordResult r = fi.get();
            assertThat(r.isSuccessful()).isTrue();
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
        assertThat(firehoseProducer.getOutstandingRecordsCount()).isEqualTo(0);
        assertThat(firehoseProducer.isFlushFailed()).isFalse();
    }

    @Test
    public void testFirehoseProducerSingleThreadTimeoutExpiredHappyCase() throws Exception {
        PutRecordBatchResult successResult = new PutRecordBatchResult();
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);

        for (int i = 0; i < 100; ++i) {
            addRecord(firehoseProducer);
        }
        Thread.sleep(2000);
        assertThat(firehoseProducer.getOutstandingRecordsCount()).isEqualTo(0);
    }

    @Test
    public void testFirehoseProducerSingleThreadBufferIsFullHappyCase() throws Exception {
        PutRecordBatchResult successResult = new PutRecordBatchResult();
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class))).thenReturn(successResult);

        for (int i = 0; i < 2 * DEFAULT_MAX_BUFFER_SIZE; ++i) {
            addRecord(firehoseProducer);
        }

        Thread.sleep(2000);
        assertThat(firehoseProducer.getOutstandingRecordsCount()).isEqualTo(0);
    }

    /**
     * This test is responsible for checking if the consumer thread has performed the work or not, so there is no way to
     * throw an exception to be catch here, so the assertion goes along the fact if the buffer was flushed or not.
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
        assertThat(firehoseProducer.getOutstandingRecordsCount()).isEqualTo(DEFAULT_MAX_BUFFER_SIZE);
        assertThat(firehoseProducer.isFlushFailed()).isTrue();
    }

    @Test
    public void testFirehoseProducerBatchesRecords() throws Exception {
        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class)))
                .thenReturn(new PutRecordBatchResult());

        // Fill up the maximum capacity: 8 * 512kB = 4MB
        IntStream.range(0, 8).forEach(i -> addRecord(firehoseProducer, KB_512));

        // Add a single byte to overflow the maximum
        addRecord(firehoseProducer, 1);

        Thread.sleep(3000);

        assertThat(firehoseProducer.getOutstandingRecordsCount()).isEqualTo(0);
        verify(firehoseClient, times(2)).putRecordBatch(putRecordCaptor.capture());

        // The first batch should contain 4 records (up to 4MB), the second should contain the remaining record
        assertThat(putRecordCaptor.getAllValues().get(0).getRecords())
                .hasSize(8).allMatch(e -> e.getData().limit() == KB_512);

        assertThat(putRecordCaptor.getAllValues().get(1).getRecords())
                .hasSize(1).allMatch(e -> e.getData().limit() == 1);
    }

    @Test
    public void testFirehoseProducerBatchesRecordsWithCustomBatchSize() throws Exception {
        Properties config = new Properties();
        config.setProperty(FIREHOSE_PRODUCER_BUFFER_MAX_BATCH_BYTES, "100");

        FirehoseProducer<UserRecordResult, Record> producer = createFirehoseProducer(config);

        when(firehoseClient.putRecordBatch(any(PutRecordBatchRequest.class)))
                .thenReturn(new PutRecordBatchResult());

        // Overflow the maximum capacity: 2 * 100kB = 200kB
        IntStream.range(0, 2).forEach(i -> addRecord(producer, 100));

        Thread.sleep(3000);

        assertThat(firehoseProducer.getOutstandingRecordsCount()).isEqualTo(0);
        verify(firehoseClient, times(2)).putRecordBatch(putRecordCaptor.capture());

        // The first batch should contain 1 record (up to 100kB), the second should contain the remaining record
        assertThat(putRecordCaptor.getAllValues().get(0).getRecords())
                .hasSize(1).allMatch(e -> e.getData().limit() == 100);

        assertThat(putRecordCaptor.getAllValues().get(1).getRecords())
                .hasSize(1).allMatch(e -> e.getData().limit() == 100);
    }

    @Test
    public void testThreadFactoryNewThreadName() {
        FirehoseThreadFactory threadFactory = new FirehoseThreadFactory();
        Thread thread1 = threadFactory.newThread(() -> LOGGER.info("Running task 1"));
        Thread thread2 = threadFactory.newThread(() -> LOGGER.info("Running task 2"));
        Thread thread3 = threadFactory.newThread(() -> LOGGER.info("Running task 3"));

        // Thread index is allocated statically, so cannot deterministically guarantee the thread number
        // Work out thread1's number and then check subsequent thread names
        int threadNumber = Integer.parseInt(thread1.getName().substring(thread1.getName().lastIndexOf('-') + 1));

        assertThat(thread1.getName()).isEqualTo("kda-writer-thread-" + threadNumber++);
        assertThat(thread1.isDaemon()).isFalse();

        assertThat(thread2.getName()).isEqualTo("kda-writer-thread-" + threadNumber++);
        assertThat(thread2.isDaemon()).isFalse();

        assertThat(thread3.getName()).isEqualTo("kda-writer-thread-" + threadNumber);
        assertThat(thread3.isDaemon()).isFalse();
    }

    @Nonnull
    private CompletableFuture<UserRecordResult> addRecord(final FirehoseProducer<UserRecordResult, Record> producer) {
        return addRecord(producer, 64);
    }

    @Nonnull
    private CompletableFuture<UserRecordResult> addRecord(final FirehoseProducer<UserRecordResult, Record> producer, final int length) {
        try {
            Record record = new Record().withData(ByteBuffer.wrap(
                    RandomStringUtils.randomAlphabetic(length).getBytes()));

            return producer.addUserRecord(record);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private FirehoseProducer<UserRecordResult, Record> createFirehoseProducer() {
        return createFirehoseProducer(new Properties());
    }

    @Nonnull
    private FirehoseProducer<UserRecordResult, Record> createFirehoseProducer(@Nonnull final Properties config) {
        config.setProperty(FIREHOSE_PRODUCER_BUFFER_MAX_TIMEOUT, "1000");
        config.setProperty(AWS_REGION, "us-east-1");
        return new FirehoseProducer<>(DEFAULT_DELIVERY_STREAM, firehoseClient, config);
    }
}
