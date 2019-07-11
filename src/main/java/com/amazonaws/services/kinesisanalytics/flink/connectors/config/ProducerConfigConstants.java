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

package com.amazonaws.services.kinesisanalytics.flink.connectors.config;

import java.util.concurrent.TimeUnit;

public class ProducerConfigConstants {

    /** The default MAX buffer size. Users should be able to specify a larger buffer if needed, since we don't bound it.
     * However, this value should be exercised with caution, since Kinesis Firehose limits PutRecordBatch at 500 records or 4MiB per call.
     * Please refer to https://docs.aws.amazon.com/firehose/latest/dev/limits.html for further reference.
     * */
    public static final int DEFAULT_MAX_BUFFER_SIZE = 500;

    /** The MAX default timeout for the buffer to be flushed */
    public static final long DEFAULT_MAX_BUFFER_TIMEOUT = TimeUnit.MINUTES.toMillis(5);

    /** The MAX default timeout for a given addUserRecord operation */
    public static final long DEFAULT_MAX_OPERATION_TIMEOUT = TimeUnit.MINUTES.toMillis(5);

    /** The default wait time in milliseconds in case a buffer is full */
    public static final long DEFAULT_WAIT_TIME_FOR_BUFFER_FULL = 100L;

    /** The default interval between buffer flushes */
    public static final long DEFAULT_INTERVAL_BETWEEN_FLUSHES = 50L;

    /** The default MAX number of retries in case of recoverable failures */
    public static final int DEFAULT_NUMBER_OF_RETRIES = 10;

    /** The default MAX backoff timeout
     * https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
     * */
    public static final long DEFAULT_MAX_BACKOFF = 100L;

    /** The default BASE timeout to be used on Jitter backoff
     * https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
     * */
    public static final long DEFAULT_BASE_BACKOFF = 10L;

    public static final String FIREHOSE_PRODUCER_BUFFER_MAX_SIZE = "firehose.producer.batch.size";
    public static final String FIREHOSE_PRODUCER_BUFFER_MAX_TIMEOUT = "firehose.producer.buffer.timeout";
    public static final String FIREHOSE_PRODUCER_BUFFER_FULL_WAIT_TIMEOUT = "firehose.producer.buffer.full.wait.timeout";
    public static final String FIREHOSE_PRODUCER_BUFFER_FLUSH_TIMEOUT = "firehose.producer.buffer.flush.timeout";
    public static final String FIREHOSE_PRODUCER_BUFFER_FLUSH_MAX_NUMBER_OF_RETRIES = "firehose.producer.buffer.flush.retries";
    public static final String FIREHOSE_PRODUCER_BUFFER_MAX_BACKOFF_TIMEOUT = "firehose.producer.buffer.max.backoff";
    public static final String FIREHOSE_PRODUCER_BUFFER_BASE_BACKOFF_TIMEOUT = "firehose.producer.buffer.base.backoff";
    public static final String FIREHOSE_PRODUCER_MAX_OPERATION_TIMEOUT = "firehose.producer.operation.timeout";
}
