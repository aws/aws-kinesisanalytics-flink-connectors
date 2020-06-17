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

import com.amazonaws.services.kinesisanalytics.flink.connectors.exception.FlinkKinesisFirehoseException;
import com.amazonaws.services.kinesisanalytics.flink.connectors.exception.RecordCouldNotBeSentException;
import com.amazonaws.services.kinesisanalytics.flink.connectors.exception.TimeoutExpiredException;
import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.IProducer;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.AmazonKinesisFirehoseException;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.ServiceUnavailableException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayDeque;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.producer.impl.FirehoseProducer.UserRecordResult;

@ThreadSafe
public class FirehoseProducer<O extends UserRecordResult, R extends Record> implements IProducer<O, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FirehoseProducer.class);

    /** A configuration object holding validated properties for the producer */
    private final FirehoseProducerConfiguration configuration;

    /** AWS Kinesis Firehose client */
    private final AmazonKinesisFirehose firehoseClient;

    /** Firehose delivery stream */
    private final String deliveryStream;

    /** Scheduler service responsible for flushing the producer Buffer pool */
    private final ExecutorService flusher;

    /** Object lock responsible for guarding the producer Buffer pool */
    @GuardedBy("this")
    private final Object producerBufferLock = new Object();

    /** Producer Buffer pool */
    private volatile Queue<Record> producerBuffer;

    /** Flusher Buffer pool */
    private volatile Queue<Record> flusherBuffer;

    /** A timestamp responsible to store the last timestamp after the flusher thread has been performed */
    private volatile long lastSucceededFlushTimestamp;

    /** Reports if the Firehose Producer was destroyed, shutting down the flusher thread. */
    private volatile boolean isDestroyed;

    /** A sentinel flag to notify the flusher thread to flush the buffer immediately.
     * This flag should be used only to request a flush from the caller thread through the {@link #flush()} method. */
    private volatile boolean syncFlush;

    /** A flag representing if the Flusher thread has failed. */
    private volatile boolean isFlusherFailed;

    public FirehoseProducer(@Nonnull final String deliveryStream,
                            @Nonnull final AmazonKinesisFirehose firehoseClient,
                            @Nonnull final Properties config) {
        this(deliveryStream, firehoseClient, FirehoseProducerConfiguration.builder(config).build());
    }

    public FirehoseProducer(@Nonnull final String deliveryStream,
                            @Nonnull final AmazonKinesisFirehose firehoseClient,
                            @Nonnull final FirehoseProducerConfiguration configuration) {

        this.firehoseClient = Validate.notNull(firehoseClient, "Kinesis Firehose client cannot be null");
        this.deliveryStream = Validate.notBlank(deliveryStream, "Kinesis Firehose delivery stream cannot be null or empty.");

        this.configuration = configuration;

        this.producerBuffer = new ArrayDeque<>(configuration.getMaxBufferSize());
        this.flusherBuffer = new ArrayDeque<>(configuration.getMaxBufferSize());

        flusher = Executors.newSingleThreadExecutor(new FirehoseThreadFactory());
        flusher.submit(this::flushBuffer);
    }

    @Override
    public CompletableFuture<O> addUserRecord(final R record) throws Exception {
        return addUserRecord(record, configuration.getMaxOperationTimeoutInMillis());
    }

    /**
     * This method is responsible for taking a lock adding a {@code Record} into the producerBuffer, in case the producerBuffer is full
     * waits releasing the lock for the given {@code bufferFullWaitTimeoutInMillis}.
     * There are cases where the producerBuffer cannot be flushed then this method keeps waiting until the given operation timeout
     * passed as {@code timeoutInMillis}
     * @param record the type of data to be buffered
     * @param timeoutInMillis the operation timeout in case the record cannot be added into the producerBuffer.
     * @return
     * @throws TimeoutExpiredException if the operation got stuck and is not able to proceed.
     * @throws InterruptedException if any thread interrupted the current thread before or while the current thread
     * was waiting for a notification.  The <i>interrupted status</i> of the current thread is cleared when
     * this exception is thrown.
     */
    @Override
    public CompletableFuture<O> addUserRecord(final R record, final long timeoutInMillis)
            throws TimeoutExpiredException, InterruptedException {

        Validate.notNull(record, "Record cannot be null.");
        Validate.isTrue(timeoutInMillis > 0, "Operation timeout should be > 0.");

        long operationTimeoutInNanos = TimeUnit.MILLISECONDS.toNanos(timeoutInMillis);

        synchronized (producerBufferLock) {
            /* This happens whenever the current thread is trying to write, however, the Producer Buffer is full.
             * This guarantees if the writer thread is already running, should wait.
             * In addition, implements a kind of back pressure mechanism with a bail out condition, so we don't incur
             * in cases where the current thread waits forever.
             */
            long lastTimestamp = System.nanoTime();
            while (producerBuffer.size() >= configuration.getMaxBufferSize()) {
                if ((System.nanoTime() - lastTimestamp) >= operationTimeoutInNanos) {
                    throw new TimeoutExpiredException("Timeout has expired for the given operation");
                }

                /* If the buffer is filled and the flusher isn't running yet we notify to wake up the flusher */
                if (flusherBuffer.isEmpty()) {
                    producerBufferLock.notify();
                }
                producerBufferLock.wait(configuration.getBufferFullWaitTimeoutInMillis());
            }

            producerBuffer.offer(record);

            /* If the buffer was filled up right after the last insertion we would like to wake up the flusher thread
             * and send the buffered data to Kinesis Firehose as soon as possible */
            if (producerBuffer.size() >= configuration.getMaxBufferSize() && flusherBuffer.isEmpty()) {
                producerBufferLock.notify();
            }
        }
        UserRecordResult recordResult = new UserRecordResult().setSuccessful(true);
        return CompletableFuture.completedFuture((O) recordResult);
    }

    /**
     * This method runs in a background thread responsible for flushing the Producer Buffer in case the buffer is full,
     * not enough records into the buffer and timeout has expired or flusher timeout has expired.
     * If an unhandled exception is thrown the flusher thread should fail, logging the failure.
     * However, this behavior will block the producer to move on until hit the given timeout and throw {@code {@link TimeoutExpiredException}}
     */
    private void flushBuffer() {

        lastSucceededFlushTimestamp = System.nanoTime();
        long bufferTimeoutInNanos = TimeUnit.MILLISECONDS.toNanos(configuration.getBufferTimeoutInMillis());
        boolean timeoutFlush;

        while (true) {
            timeoutFlush = (System.nanoTime() - lastSucceededFlushTimestamp) >= bufferTimeoutInNanos;

            synchronized (producerBufferLock) {

                /* If the flusher buffer is not empty at this point we should fail, otherwise we would end up looping
                 * forever since we are swapping references */
                Validate.validState(flusherBuffer.isEmpty());

                if (isDestroyed) {
                    return;
                } else if (syncFlush || (producerBuffer.size() >= configuration.getMaxBufferSize() ||
                        (timeoutFlush && producerBuffer.size() > 0))) {
                    prepareRecordsToSubmit(producerBuffer, flusherBuffer);
                    producerBufferLock.notify();
                } else {
                    try {
                        producerBufferLock.wait(configuration.getBufferTimeoutBetweenFlushes());
                    } catch (InterruptedException e) {
                        LOGGER.info("An interrupted exception has been thrown, while trying to sleep and release the lock during a flush.", e);
                    }
                    continue;
                }
            }
            /* It's OK calling {@code submitBatchWithRetry} outside the critical section because this method does not make
             * any changes to the object and the producer thread does not make any modifications to the flusherBuffer.
             * The only agent making changes to flusherBuffer is the flusher thread. */
            try {
                submitBatchWithRetry(flusherBuffer);

                Queue<Record> emptyFlushBuffer = new ArrayDeque<>(configuration.getMaxBufferSize());
                synchronized (producerBufferLock) {
                    /* We perform a swap at this point because {@code ArrayDeque<>.clear()} iterates over the items nullifying
                     * the items, and we would like to avoid such iteration just swapping references. */
                    Validate.validState(!flusherBuffer.isEmpty());
                    flusherBuffer = emptyFlushBuffer;

                    if (syncFlush) {
                        syncFlush = false;
                        producerBufferLock.notify();
                    }
                }

            } catch (Exception ex) {
                String errorMsg = "An error has occurred while trying to send data to Kinesis Firehose.";

                if (ex instanceof AmazonKinesisFirehoseException &&
                        ((AmazonKinesisFirehoseException) ex).getStatusCode() == 413) {

                    LOGGER.error(errorMsg +
                            "Batch of records too large. Please try to reduce your batch size by passing " +
                            "FIREHOSE_PRODUCER_BUFFER_MAX_SIZE into your configuration.", ex);

                } else {
                    LOGGER.error(errorMsg, ex);
                }

                synchronized (producerBufferLock) {
                    isFlusherFailed = true;
                }

                throw ex;
            }
        }
    }

    /**
     * Populates the target queue with messages from the source queue.
     * Up to the maximum capacity defined by {@code maxPutRecordBatchBytes}.
     */
    private void prepareRecordsToSubmit(@Nonnull final Queue<Record> sourceQueue, @Nonnull final Queue<Record> targetQueue) {
        int total = 0;
        while (!sourceQueue.isEmpty() && (total + sourceQueue.peek().getData().capacity()) <= configuration.getMaxPutRecordBatchBytes()) {
            total += sourceQueue.peek().getData().capacity();
            targetQueue.add(sourceQueue.poll());
        }
    }

    private void submitBatchWithRetry(final Queue<Record> records) throws AmazonKinesisFirehoseException,
            RecordCouldNotBeSentException {

        PutRecordBatchResult lastResult;
        String warnMessage = null;
        for (int attempts = 0; attempts < configuration.getNumberOfRetries(); attempts++) {
            try {
                LOGGER.debug("Trying to flush Buffer of size: {} on attempt: {}", records.size(), attempts);

                lastResult = submitBatch(records);

                if (lastResult.getFailedPutCount() == null || lastResult.getFailedPutCount() == 0) {

                    lastSucceededFlushTimestamp = System.nanoTime();
                    LOGGER.debug("Firehose Buffer has been flushed with size: {} on attempt: {}",
                            records.size(), attempts);
                    return;
                }

                PutRecordBatchResponseEntry failedRecord = lastResult.getRequestResponses()
                        .stream()
                        .filter(r -> r.getRecordId() == null)
                        .findFirst()
                        .orElse(null);

                warnMessage = String.format("Number of failed records: %s.", lastResult.getFailedPutCount());
                if (failedRecord != null) {
                    warnMessage = String.format("Last Kinesis Firehose putRecordBatch encountered an error and failed " +
                                    "trying to put: %s records with error: %s - %s.",
                            lastResult.getFailedPutCount(), failedRecord.getErrorCode(), failedRecord.getErrorMessage());
                }
                LOGGER.warn(warnMessage);

                //Full Jitter: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
                long timeToSleep = RandomUtils.nextLong(0,
                        Math.min(configuration.getMaxBackOffInMillis(), (configuration.getBaseBackOffInMillis() * 2 * attempts)));
                LOGGER.info("Sleeping for: {}ms on attempt: {}", timeToSleep, attempts);
                Thread.sleep(timeToSleep);

            } catch (ServiceUnavailableException ex) {
                LOGGER.info("Kinesis Firehose has thrown a recoverable exception.", ex);
            } catch (InterruptedException e) {
                LOGGER.info("An interrupted exception has been thrown between retry attempts.", e);
            } catch (AmazonKinesisFirehoseException ex) {
                throw ex;
            }
        }

        throw new RecordCouldNotBeSentException("Exceeded number of attempts! " + warnMessage);
    }

    /**
     * Sends the actual batch of records to Kinesis Firehose
     * @param records a Collection of records
     * @return {@code PutRecordBatchResult}
     */
    private PutRecordBatchResult submitBatch(final Queue<Record> records) throws AmazonKinesisFirehoseException {

        LOGGER.debug("Sending {} records to Kinesis Firehose on stream: {}", records.size(), deliveryStream);

        PutRecordBatchResult result;
        try {
            result = firehoseClient.putRecordBatch(new PutRecordBatchRequest()
                    .withDeliveryStreamName(deliveryStream)
                    .withRecords(records));
        } catch (AmazonKinesisFirehoseException e) {
            throw e;
        }
        return result;
    }

    /**
     * Make sure that any pending scheduled thread terminates before closing as well as cleans the producerBuffer pool,
     * allowing GC to collect.
     */
    @Override
    public void destroy() throws Exception {

        synchronized (producerBufferLock) {
            isDestroyed = true;
            producerBuffer = null;
            producerBufferLock.notify();
        }

        if (!flusher.isShutdown() && !flusher.isTerminated()) {
            LOGGER.info("Shutting down scheduled service.");
            flusher.shutdown();
            try {
                LOGGER.info("Awaiting executor service termination...");
                flusher.awaitTermination(1L, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                final String errorMsg = "Error waiting executor writer termination.";
                LOGGER.error(errorMsg, e);
                throw new FlinkKinesisFirehoseException(errorMsg, e);
            }
        }
    }

    @Override
    public boolean isDestroyed() {
        synchronized (producerBufferLock) {
            return isDestroyed;
        }
    }

    @Override
    public int getOutstandingRecordsCount() {
        synchronized (producerBufferLock) {
            return producerBuffer.size() + flusherBuffer.size();
        }
    }

    @Override
    public boolean isFlushFailed() {
        synchronized (producerBufferLock) {
            return isFlusherFailed;
        }
    }

    /**
     * This method instructs the flusher thread to perform a flush on the buffer without waiting for completion.
     * <p>
     *     This implementation does not guarantee the whole buffer is flushed or if the flusher thread
     *     has completed the flush or not.
     *     In order to flush all records and wait until completion, use {@code {@link #flushSync()}}
     * </p>
     */
    @Override
    public void flush() {
        synchronized (producerBufferLock) {
            syncFlush = true;
            producerBufferLock.notify();
        }
    }

    /**
     * This method instructs the flusher thread to perform the flush on the buffer and wait for the completion.
     * <p>
     *     This implementation is useful once there is a need to guarantee the buffer is flushed before making further progress.
     *     i.e. Shutting down the producer.
     *     i.e. Taking synchronous snapshots.
     * </p>
     * The caller needs to make sure to assert the status of {@link #isFlushFailed()} in order guarantee whether
     * the flush has successfully completed or not.
     */
    @Override
    public void flushSync() {
        while (getOutstandingRecordsCount() > 0 && !isFlushFailed()) {
            flush();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                LOGGER.warn("An interruption has happened while trying to flush the buffer synchronously.");
                Thread.currentThread().interrupt();
            }
        }

        if (isFlushFailed()) {
            LOGGER.warn("The flusher thread has failed trying to synchronously flush the buffer.");
        }
    }

    public static class UserRecordResult {
        private Throwable exception;
        private boolean successful;

        public Throwable getException() {
            return exception;
        }

        public UserRecordResult setException(Throwable exception) {
            this.exception = exception;
            return this;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public UserRecordResult setSuccessful(boolean successful) {
            this.successful = successful;
            return this;
        }
    }

    static class FirehoseThreadFactory implements ThreadFactory {
        /** Static threadsafe counter use to generate thread name suffix. */
        private static final AtomicLong count = new AtomicLong(0);

        @Override
        public Thread newThread(@Nonnull final Runnable runnable) {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName("kda-writer-thread-" + count.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        }
    }
}
