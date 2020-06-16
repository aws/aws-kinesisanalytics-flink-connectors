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

import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants;
import com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants;
import com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil;
import org.apache.commons.lang3.Validate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_MAXIMUM_BATCH_BYTES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_MAX_BUFFER_SIZE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_BASE_BACKOFF_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_FLUSH_MAX_NUMBER_OF_RETRIES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_FLUSH_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_FULL_WAIT_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_BACKOFF_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_BATCH_BYTES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_SIZE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_MAX_OPERATION_TIMEOUT;
import static java.util.Optional.ofNullable;

/** An immutable configuration class for {@link FirehoseProducer}. */
public class FirehoseProducerConfiguration {

    /** The default MAX producerBuffer size. Users should be able to specify a smaller producerBuffer if needed.
     * However, this value should be exercised with caution, since Kinesis Firehose limits PutRecordBatch at 500 records or 4MiB per call.
     * Please refer to https://docs.aws.amazon.com/firehose/latest/dev/limits.html for further reference.
     * */
    private final int maxBufferSize;

    /** The maximum number of bytes that can be sent in a single PutRecordBatch operation */
    private final int maxPutRecordBatchBytes;

    /** The specified amount timeout the producerBuffer must be flushed if haven't met any other conditions previously */
    private final long bufferTimeoutInMillis;

    /** The wait time in milliseconds in case a producerBuffer is full */
    private final long bufferFullWaitTimeoutInMillis;

    /** The interval between producerBuffer flushes */
    private final long bufferTimeoutBetweenFlushes;

    /** The MAX number of retries in case of recoverable failures */
    private final int numberOfRetries;

    /** The default MAX backoff timeout
     * https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
     */
    private final long maxBackOffInMillis;

    /** The default BASE timeout to be used on Jitter backoff
     * https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
     */
    private final long baseBackOffInMillis;

    /** The MAX timeout for a given addUserRecord operation */
    private final long maxOperationTimeoutInMillis;

    private FirehoseProducerConfiguration(@Nonnull final Builder builder) {
        this.maxBufferSize = builder.maxBufferSize;
        this.maxPutRecordBatchBytes = builder.maxPutRecordBatchBytes;
        this.bufferTimeoutInMillis = builder.bufferTimeoutInMillis;
        this.bufferFullWaitTimeoutInMillis = builder.bufferFullWaitTimeoutInMillis;
        this.bufferTimeoutBetweenFlushes = builder.bufferTimeoutBetweenFlushes;
        this.numberOfRetries = builder.numberOfRetries;
        this.maxBackOffInMillis = builder.maxBackOffInMillis;
        this.baseBackOffInMillis = builder.baseBackOffInMillis;
        this.maxOperationTimeoutInMillis = builder.maxOperationTimeoutInMillis;
    }

    /**
     * The max producer buffer size; the maximum number of records that will be sent in a PutRecordBatch request.
     * @return the max producer buffer size.
     */
    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    /**
     * The maximum number of bytes that will be sent in a single PutRecordBatch operation.
     * @return the maximum number of PutRecordBatch bytes
     */
    public int getMaxPutRecordBatchBytes() {
        return maxPutRecordBatchBytes;
    }

    /**
     * The specified amount timeout the producerBuffer must be flushed if haven't met any other conditions previously.
     * @return the specified amount timeout the producerBuffer must be flushed
     */
    public long getBufferTimeoutInMillis() {
        return bufferTimeoutInMillis;
    }

    /**
     * The wait time in milliseconds in case a producerBuffer is full.
     * @return The wait time in milliseconds
     */
    public long getBufferFullWaitTimeoutInMillis() {
        return bufferFullWaitTimeoutInMillis;
    }

    /**
     * The interval between producerBuffer flushes.
     * @return The interval between producerBuffer flushes
     */
    public long getBufferTimeoutBetweenFlushes() {
        return bufferTimeoutBetweenFlushes;
    }

    /**
     * The max number of retries in case of recoverable failures.
     * @return the max number of retries in case of recoverable failures
     */
    public int getNumberOfRetries() {
        return numberOfRetries;
    }

    /**
     * The max backoff timeout (https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
     * @return The max backoff timeout
     */
    public long getMaxBackOffInMillis() {
        return maxBackOffInMillis;
    }

    /**
     * The base backoff timeout (https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
     * @return The base backoff timeout
     */
    public long getBaseBackOffInMillis() {
        return baseBackOffInMillis;
    }

    /**
     * The max timeout for a given addUserRecord operation.
     * @return the max timeout for a given addUserRecord operation
     */
    public long getMaxOperationTimeoutInMillis() {
        return maxOperationTimeoutInMillis;
    }

    @Nonnull
    public static Builder builder(@Nonnull final Properties config) {
        final String region = config.getProperty(AWSConfigConstants.AWS_REGION);
        return builder(region).withProperties(config);
    }

    @Nonnull
    public static Builder builder(@Nullable final String region) {
        return new Builder(region);
    }

    public static class Builder {
        private int maxBufferSize = ProducerConfigConstants.DEFAULT_MAX_BUFFER_SIZE;
        private int maxPutRecordBatchBytes;
        private int numberOfRetries = ProducerConfigConstants.DEFAULT_NUMBER_OF_RETRIES;
        private long bufferTimeoutInMillis = ProducerConfigConstants.DEFAULT_MAX_BUFFER_TIMEOUT;
        private long maxOperationTimeoutInMillis = ProducerConfigConstants.DEFAULT_MAX_OPERATION_TIMEOUT;
        private long bufferFullWaitTimeoutInMillis = ProducerConfigConstants.DEFAULT_WAIT_TIME_FOR_BUFFER_FULL;
        private long bufferTimeoutBetweenFlushes = ProducerConfigConstants.DEFAULT_INTERVAL_BETWEEN_FLUSHES;
        private long maxBackOffInMillis = ProducerConfigConstants.DEFAULT_MAX_BACKOFF;
        private long baseBackOffInMillis = ProducerConfigConstants.DEFAULT_BASE_BACKOFF;

        public Builder(@Nullable final String region) {
            this.maxPutRecordBatchBytes = AWSUtil.getDefaultMaxPutRecordBatchBytes(region);
        }

        @Nonnull
        public FirehoseProducerConfiguration build() {
            return new FirehoseProducerConfiguration(this);
        }

        /**
         * The max producer buffer size; the maximum number of records that will be sent in a PutRecordBatch request.
         * @param maxBufferSize the max producer buffer size
         * @return this builder
         */
        @Nonnull
        public Builder withMaxBufferSize(final int maxBufferSize) {
            Validate.isTrue(maxBufferSize > 0 && maxBufferSize <= DEFAULT_MAX_BUFFER_SIZE,
                    String.format("Buffer size must be between 1 and %d", DEFAULT_MAX_BUFFER_SIZE));

            this.maxBufferSize = maxBufferSize;
            return this;
        }

        /**
         * The maximum number of bytes that will be sent in a single PutRecordBatch operation.
         * @param maxPutRecordBatchBytes the maximum number of PutRecordBatch bytes
         * @return this builder
         */
        @Nonnull
        public Builder withMaxPutRecordBatchBytes(final int maxPutRecordBatchBytes) {
            Validate.isTrue(maxPutRecordBatchBytes > 0 && maxPutRecordBatchBytes <= DEFAULT_MAXIMUM_BATCH_BYTES,
                    String.format("Maximum batch size in bytes must be between 1 and %d", DEFAULT_MAXIMUM_BATCH_BYTES));

            this.maxPutRecordBatchBytes = maxPutRecordBatchBytes;
            return this;
        }

        /**
         * The max number of retries in case of recoverable failures.
         * @param numberOfRetries the max number of retries in case of recoverable failures.
         * @return this builder
         */
        @Nonnull
        public Builder withNumberOfRetries(final int numberOfRetries) {
            Validate.isTrue(numberOfRetries >= 0, "Number of retries cannot be negative.");

            this.numberOfRetries = numberOfRetries;
            return this;
        }

        /**
         * The specified amount timeout the producerBuffer must be flushed if haven't met any other conditions previously.
         * @param bufferTimeoutInMillis the specified amount timeout the producerBuffer must be flushed
         * @return this builder
         */
        @Nonnull
        public Builder withBufferTimeoutInMillis(final long bufferTimeoutInMillis) {
            Validate.isTrue(bufferTimeoutInMillis >= 0, "Flush timeout should be greater than 0.");

            this.bufferTimeoutInMillis = bufferTimeoutInMillis;
            return this;
        }

        /**
         * The max timeout for a given addUserRecord operation.
         * @param maxOperationTimeoutInMillis The max timeout for a given addUserRecord operation
         * @return this builder
         */
        @Nonnull
        public Builder withMaxOperationTimeoutInMillis(final long maxOperationTimeoutInMillis) {
            Validate.isTrue(maxOperationTimeoutInMillis >= 0, "Max operation timeout should be greater than 0.");

            this.maxOperationTimeoutInMillis = maxOperationTimeoutInMillis;
            return this;
        }

        /**
         * The wait time in milliseconds in case a producerBuffer is full.
         * @param bufferFullWaitTimeoutInMillis the wait time in milliseconds in case a producerBuffer is full
         * @return this builder
         */
        @Nonnull
        public Builder withBufferFullWaitTimeoutInMillis(final long bufferFullWaitTimeoutInMillis) {
            Validate.isTrue(bufferFullWaitTimeoutInMillis >= 0, "Buffer full waiting timeout should be greater than 0.");

            this.bufferFullWaitTimeoutInMillis = bufferFullWaitTimeoutInMillis;
            return this;
        }

        /**
         * The interval between producerBuffer flushes.
         * @param bufferTimeoutBetweenFlushes the interval between producerBuffer flushes
         * @return this builder
         */
        @Nonnull
        public Builder withBufferTimeoutBetweenFlushes(final long bufferTimeoutBetweenFlushes) {
            Validate.isTrue(bufferTimeoutBetweenFlushes >= 0, "Interval between flushes cannot be negative.");

            this.bufferTimeoutBetweenFlushes = bufferTimeoutBetweenFlushes;
            return this;
        }

        /**
         * The max backoff timeout (https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
         * @param maxBackOffInMillis the max backoff timeout
         * @return this builder
         */
        @Nonnull
        public Builder withMaxBackOffInMillis(final long maxBackOffInMillis) {
            Validate.isTrue(maxBackOffInMillis >= 0, "Max backoff timeout should be greater than 0.");

            this.maxBackOffInMillis = maxBackOffInMillis;
            return this;
        }

        /**
         * The base backoff timeout (https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
         * @param baseBackOffInMillis The base backoff timeout
         * @return this builder
         */
        @Nonnull
        public Builder withBaseBackOffInMillis(final long baseBackOffInMillis) {
            Validate.isTrue(baseBackOffInMillis >= 0, "Base backoff timeout should be greater than 0.");

            this.baseBackOffInMillis = baseBackOffInMillis;
            return this;
        }

        /**
         * Creates a Builder populated with values from the Properties.
         * @param config the configuration properties
         * @return this builder
         */
        @Nonnull
        public Builder withProperties(@Nonnull final Properties config) {
            ofNullable(config.getProperty(FIREHOSE_PRODUCER_BUFFER_MAX_SIZE))
                    .map(Integer::parseInt)
                    .ifPresent(this::withMaxBufferSize);

            ofNullable(config.getProperty(FIREHOSE_PRODUCER_BUFFER_MAX_BATCH_BYTES))
                    .map(Integer::parseInt)
                    .ifPresent(this::withMaxPutRecordBatchBytes);

            ofNullable(config.getProperty(FIREHOSE_PRODUCER_BUFFER_FLUSH_MAX_NUMBER_OF_RETRIES))
                    .map(Integer::parseInt)
                    .ifPresent(this::withNumberOfRetries);

            ofNullable(config.getProperty(FIREHOSE_PRODUCER_BUFFER_MAX_TIMEOUT))
                    .map(Long::parseLong)
                    .ifPresent(this::withBufferTimeoutInMillis);

            ofNullable(config.getProperty(FIREHOSE_PRODUCER_BUFFER_FULL_WAIT_TIMEOUT))
                    .map(Long::parseLong)
                    .ifPresent(this::withBufferFullWaitTimeoutInMillis);

            ofNullable(config.getProperty(FIREHOSE_PRODUCER_BUFFER_FLUSH_TIMEOUT))
                    .map(Long::parseLong)
                    .ifPresent(this::withBufferTimeoutBetweenFlushes);

            ofNullable(config.getProperty(FIREHOSE_PRODUCER_BUFFER_MAX_BACKOFF_TIMEOUT))
                    .map(Long::parseLong)
                    .ifPresent(this::withMaxBackOffInMillis);

            ofNullable(config.getProperty(FIREHOSE_PRODUCER_BUFFER_BASE_BACKOFF_TIMEOUT))
                    .map(Long::parseLong)
                    .ifPresent(this::withBaseBackOffInMillis);

            ofNullable(config.getProperty(FIREHOSE_PRODUCER_MAX_OPERATION_TIMEOUT))
                    .map(Long::parseLong)
                    .ifPresent(this::withMaxOperationTimeoutInMillis);

            return this;
        }
    }
}
