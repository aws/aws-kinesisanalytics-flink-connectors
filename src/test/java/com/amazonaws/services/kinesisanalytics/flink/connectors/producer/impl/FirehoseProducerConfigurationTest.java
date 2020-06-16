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

import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_BASE_BACKOFF;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_INTERVAL_BETWEEN_FLUSHES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_MAXIMUM_BATCH_BYTES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_MAX_BACKOFF;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_MAX_BUFFER_SIZE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_MAX_BUFFER_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_MAX_OPERATION_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_NUMBER_OF_RETRIES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.DEFAULT_WAIT_TIME_FOR_BUFFER_FULL;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_BASE_BACKOFF_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_FLUSH_MAX_NUMBER_OF_RETRIES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_FLUSH_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_FULL_WAIT_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_BACKOFF_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_BATCH_BYTES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_SIZE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_BUFFER_MAX_TIMEOUT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.ProducerConfigConstants.FIREHOSE_PRODUCER_MAX_OPERATION_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FirehoseProducerConfigurationTest {
    private static final String REGION = "us-east-1";

    @Test
    public void testBuilderWithDefaultProperties() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration.builder(REGION).build();

        assertThat(configuration.getMaxBufferSize()).isEqualTo(DEFAULT_MAX_BUFFER_SIZE);
        assertThat(configuration.getMaxPutRecordBatchBytes()).isEqualTo(DEFAULT_MAXIMUM_BATCH_BYTES);
        assertThat(configuration.getNumberOfRetries()).isEqualTo(DEFAULT_NUMBER_OF_RETRIES);
        assertThat(configuration.getBufferFullWaitTimeoutInMillis()).isEqualTo(DEFAULT_WAIT_TIME_FOR_BUFFER_FULL);
        assertThat(configuration.getBufferTimeoutInMillis()).isEqualTo(DEFAULT_MAX_BUFFER_TIMEOUT);
        assertThat(configuration.getBufferTimeoutBetweenFlushes()).isEqualTo(DEFAULT_INTERVAL_BETWEEN_FLUSHES);
        assertThat(configuration.getMaxBackOffInMillis()).isEqualTo(DEFAULT_MAX_BACKOFF);
        assertThat(configuration.getBaseBackOffInMillis()).isEqualTo(DEFAULT_BASE_BACKOFF);
        assertThat(configuration.getMaxOperationTimeoutInMillis()).isEqualTo(DEFAULT_MAX_OPERATION_TIMEOUT);
    }

    @Test
    public void testBuilderWithMaxBufferSize() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withMaxBufferSize(250)
                .build();

        assertThat(configuration.getMaxBufferSize()).isEqualTo(250);
    }

    @Test
    public void testBuilderWithMaxBufferSizeRejectsZero() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> FirehoseProducerConfiguration.builder(REGION).withMaxBufferSize(0))
                .withMessageContaining("Buffer size must be between 1 and 500");
    }

    @Test
    public void testBuilderWithMaxBufferSizeRejectsUpperLimit() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> FirehoseProducerConfiguration.builder(REGION).withMaxBufferSize(501))
                .withMessageContaining("Buffer size must be between 1 and 500");
    }

    @Test
    public void testBuilderWithMaxPutRecordBatchBytes() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withMaxPutRecordBatchBytes(100)
                .build();

        assertThat(configuration.getMaxPutRecordBatchBytes()).isEqualTo(100);
    }

    @Test
    public void testBuilderWithMaxPutRecordBatchBytesRejectsZero() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> FirehoseProducerConfiguration.builder(REGION).withMaxPutRecordBatchBytes(0))
                .withMessageContaining("Maximum batch size in bytes must be between 1 and 4194304");
    }

    @Test
    public void testBuilderWithMaxPutRecordBatchBytesRejectsUpperLimit() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> FirehoseProducerConfiguration.builder(REGION).withMaxPutRecordBatchBytes(4194305))
                .withMessageContaining("Maximum batch size in bytes must be between 1 and 4194304");
    }

    @Test
    public void testBuilderWithNumberOfRetries() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withNumberOfRetries(100)
                .build();

        assertThat(configuration.getNumberOfRetries()).isEqualTo(100);
    }

    @Test
    public void testBuilderWithNumberOfRetriesRejectsNegative() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> FirehoseProducerConfiguration.builder(REGION).withNumberOfRetries(-1))
                .withMessageContaining("Number of retries cannot be negative");
    }

    @Test
    public void testBuilderWithBufferTimeoutInMillis() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withBufferTimeoutInMillis(12345L)
                .build();

        assertThat(configuration.getBufferTimeoutInMillis()).isEqualTo(12345L);
    }

    @Test
    public void testBuilderWithBufferTimeoutInMillisRejects() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> FirehoseProducerConfiguration.builder(REGION).withBufferTimeoutInMillis(-1))
                .withMessageContaining("Flush timeout should be greater than 0");
    }

    @Test
    public void testBuilderWithMaxOperationTimeoutInMillis() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withMaxOperationTimeoutInMillis(999L)
                .build();

        assertThat(configuration.getMaxOperationTimeoutInMillis()).isEqualTo(999L);
    }

    @Test
    public void testBuilderWithMaxOperationTimeoutInMillisRejectsNegative() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> FirehoseProducerConfiguration.builder(REGION).withMaxOperationTimeoutInMillis(-1))
                .withMessageContaining("Max operation timeout should be greater than 0");
    }

    @Test
    public void testBuilderWithBufferFullWaitTimeoutInMillis() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withBufferFullWaitTimeoutInMillis(1L)
                .build();

        assertThat(configuration.getBufferFullWaitTimeoutInMillis()).isEqualTo(1L);
    }

    @Test
    public void testBuilderWithBufferFullWaitTimeoutInMillisRejectsNegative() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> FirehoseProducerConfiguration.builder(REGION).withBufferFullWaitTimeoutInMillis(-1))
                .withMessageContaining("Buffer full waiting timeout should be greater than 0");
    }

    @Test
    public void testBuilderWithBufferTimeoutBetweenFlushes() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withBufferTimeoutBetweenFlushes(2L)
                .build();

        assertThat(configuration.getBufferTimeoutBetweenFlushes()).isEqualTo(2L);
    }

    @Test
    public void testBuilderWithBufferTimeoutBetweenFlushesRejectsNegative() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> FirehoseProducerConfiguration.builder(REGION).withBufferTimeoutBetweenFlushes(-1))
                .withMessageContaining("Interval between flushes cannot be negative");
    }

    @Test
    public void testBuilderWithMaxBackOffInMillis() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withMaxBackOffInMillis(3L)
                .build();

        assertThat(configuration.getMaxBackOffInMillis()).isEqualTo(3L);
    }

    @Test
    public void testBuilderWithMaxBackOffInMillisRejectsNegative() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> FirehoseProducerConfiguration.builder(REGION).withMaxBackOffInMillis(-1))
                .withMessageContaining("Max backoff timeout should be greater than 0");
    }

    @Test
    public void testBuilderWithBaseBackOffInMillis() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withBaseBackOffInMillis(4L)
                .build();

        assertThat(configuration.getBaseBackOffInMillis()).isEqualTo(4L);
    }

    @Test
    public void testBuilderWithBaseBackOffInMillisRejectsNegative() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> FirehoseProducerConfiguration.builder(REGION).withBaseBackOffInMillis(-1))
                .withMessageContaining("Base backoff timeout should be greater than 0");
    }

    @Test
    public void testBuilderWithMaxBufferSizeFromProperties() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withProperties(props(FIREHOSE_PRODUCER_BUFFER_MAX_SIZE, "250"))
                .build();

        assertThat(configuration.getMaxBufferSize()).isEqualTo(250);
    }

    @Test
    public void testBuilderWithMaxPutRecordBatchBytesFromProperties() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withProperties(props(FIREHOSE_PRODUCER_BUFFER_MAX_BATCH_BYTES, "100"))
                .build();

        assertThat(configuration.getMaxPutRecordBatchBytes()).isEqualTo(100);
    }

    @Test
    public void testBuilderWithNumberOfRetriesFromProperties() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withProperties(props(FIREHOSE_PRODUCER_BUFFER_FLUSH_MAX_NUMBER_OF_RETRIES, "100"))
                .build();

        assertThat(configuration.getNumberOfRetries()).isEqualTo(100);
    }

    @Test
    public void testBuilderWithBufferTimeoutInMillisFromProperties() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withProperties(props(FIREHOSE_PRODUCER_BUFFER_MAX_TIMEOUT, "12345"))
                .build();

        assertThat(configuration.getBufferTimeoutInMillis()).isEqualTo(12345L);
    }

    @Test
    public void testBuilderWithMaxOperationTimeoutInMillisFromProperties() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withProperties(props(FIREHOSE_PRODUCER_MAX_OPERATION_TIMEOUT, "999"))
                .build();

        assertThat(configuration.getMaxOperationTimeoutInMillis()).isEqualTo(999L);
    }

    @Test
    public void testBuilderWithBufferFullWaitTimeoutInMillisFromProperties() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withProperties(props(FIREHOSE_PRODUCER_BUFFER_FULL_WAIT_TIMEOUT, "1"))
                .build();

        assertThat(configuration.getBufferFullWaitTimeoutInMillis()).isEqualTo(1L);
    }

    @Test
    public void testBuilderWithBufferTimeoutBetweenFlushesFromProperties() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withProperties(props(FIREHOSE_PRODUCER_BUFFER_FLUSH_TIMEOUT, "2"))
                .build();

        assertThat(configuration.getBufferTimeoutBetweenFlushes()).isEqualTo(2L);
    }

    @Test
    public void testBuilderWithMaxBackOffInMillisFromProperties() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withProperties(props(FIREHOSE_PRODUCER_BUFFER_MAX_BACKOFF_TIMEOUT, "3"))
                .build();

        assertThat(configuration.getMaxBackOffInMillis()).isEqualTo(3L);
    }

    @Test
    public void testBuilderWithBaseBackOffInMillisFromProperties() {
        FirehoseProducerConfiguration configuration = FirehoseProducerConfiguration
                .builder(REGION)
                .withProperties(props(FIREHOSE_PRODUCER_BUFFER_BASE_BACKOFF_TIMEOUT, "4"))
                .build();

        assertThat(configuration.getBaseBackOffInMillis()).isEqualTo(4L);
    }

    @Nonnull
    private Properties props(@Nonnull final String key, @Nonnull final String value) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return properties;
    }
}