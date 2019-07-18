package com.amazonaws.services.kinesisanalytics.flink.connectors.producer;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface responsible for sending data a specific sink
 */
public interface IProducer<O, R> {

    /**
     * This method should send data to an specific destination.
     * @param record the type of data to be sent
     * @return a {@code ListenableFuture} with the result for the operation.
     * @throws Exception
     */
    ListenableFuture<O> addUserRecord(final R record) throws Exception;

    /**
     * This method should send data to an specific destination
     * @param record the type of data to be sent
     * @param operationTimeoutInMillis the expected operation timeout
     * @return a {@code ListenableFuture} with the result for the operation.
     * @throws Exception
     */
    ListenableFuture<O> addUserRecord(final R record, final long operationTimeoutInMillis) throws Exception;

    /**
     * Destroy and release any used resource.
     * @throws Exception
     */
    void destroy() throws Exception;

    /**
     * Returns whether the producer has been destroyed or not
     * @return
     */
    boolean isDestroyed();

    /**
     * Should return the number of outstanding records if the producer implements buffering.
     * @return an integer with the number of outstanding records.
     */
    int getOutstandingRecordsCount();

    /**
     * This method flushes the buffer immediately.
     */
    void flush();

    /**
     * Performs a synchronous flush on the buffer waiting until the whole buffer is drained.
     */
    void flushSync();

    /**
     * A flag representing whether the flush has failed or not.
     * @return {@code boolean} representing whether the success of failure of flush buffer operation.
     */
    boolean isFlushFailed();
}
