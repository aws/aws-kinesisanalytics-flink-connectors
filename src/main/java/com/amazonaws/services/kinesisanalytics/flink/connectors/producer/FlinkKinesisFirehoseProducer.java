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

package com.amazonaws.services.kinesisanalytics.flink.connectors.producer;


import com.amazonaws.services.kinesisanalytics.flink.connectors.exception.FlinkKinesisFirehoseException;
import com.amazonaws.services.kinesisanalytics.flink.connectors.exception.RecordCouldNotBeSentException;
import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.impl.FirehoseProducer;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.CredentialProvider;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.factory.CredentialProviderFactory;
import com.amazonaws.services.kinesisanalytics.flink.connectors.serialization.KinesisFirehoseSerializationSchema;
import com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.Record;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.producer.impl.FirehoseProducer.UserRecordResult;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.getCredentialProviderType;

public class FlinkKinesisFirehoseProducer<OUT> extends RichSinkFunction<OUT> implements CheckpointedFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKinesisFirehoseProducer.class);

    private final KinesisFirehoseSerializationSchema<OUT> schema;
    private final Properties config;
    private final CredentialProviderType credentialProviderType;

    /** Name of the default delivery stream to produce to. Can be overwritten by the serialization schema */
    private final String defaultDeliveryStream;

    /** Specify whether stop and fail in case of an error */
    private boolean failOnError;

    /** Remembers the last Async thrown exception */
    private transient volatile Throwable lastThrownException;

    /** The Crendential provider should be not serialized */
    private transient CredentialProvider credentialsProvider;

    /** AWS client cannot be serialized when building the Flink Job graph */
    private transient AmazonKinesisFirehose firehoseClient;

    /** AWS Kinesis Firehose producer */
    private transient IProducer<UserRecordResult, Record> firehoseProducer;

    /**
     * Creates a new Flink Kinesis Firehose Producer.
     * @param deliveryStream The AWS Kinesis Firehose delivery stream.
     * @param schema The Serialization schema for the given data type.
     * @param configProps The properties used to configure Kinesis Firehose client.
     * @param credentialProviderType The specified Credential Provider type.
     */
    public FlinkKinesisFirehoseProducer(final String deliveryStream,
                                        final KinesisFirehoseSerializationSchema<OUT> schema,
                                        final Properties configProps,
                                        final CredentialProviderType credentialProviderType) {
        this.defaultDeliveryStream = Validate.notBlank(deliveryStream, "Delivery stream cannot be null or empty");
        this.schema = Validate.notNull(schema, "Kinesis serialization schema cannot be null");
        this.config = Validate.notNull(configProps, "Configuration properties cannot be null");
        this.credentialProviderType = Validate.notNull(credentialProviderType,
                "Credential Provider type cannot be null");
    }

    public FlinkKinesisFirehoseProducer(final String deliveryStream , final SerializationSchema<OUT> schema,
                                        final Properties configProps,
                                        final CredentialProviderType credentialProviderType) {
        this(deliveryStream, new KinesisFirehoseSerializationSchema<OUT>() {
            @Override
            public ByteBuffer serialize(OUT element) {
                return ByteBuffer.wrap(schema.serialize(element));
            }
        }, configProps, credentialProviderType);
    }

    public FlinkKinesisFirehoseProducer(final String deliveryStream, final KinesisFirehoseSerializationSchema<OUT> schema,
                                        final Properties configProps) {
        this(deliveryStream, schema, configProps, getCredentialProviderType(configProps, AWS_CREDENTIALS_PROVIDER));
    }

    public FlinkKinesisFirehoseProducer(final String deliveryStream, final SerializationSchema<OUT> schema,
                                        final Properties configProps) {
        this(deliveryStream, schema, configProps, getCredentialProviderType(configProps, AWS_CREDENTIALS_PROVIDER));
    }

    public void setFailOnError(final boolean failOnError) {
        this.failOnError = failOnError;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.credentialsProvider = CredentialProviderFactory.newCredentialProvider(credentialProviderType, config);
        LOGGER.info("Credential provider: {}", credentialsProvider.getAwsCredentialsProvider().getClass().getName() );

        this.firehoseClient = createKinesisFirehoseClient();
        this.firehoseProducer = createFirehoseProducer();

        LOGGER.info("Started Kinesis Firehose client. Delivering to stream: {}", defaultDeliveryStream);
    }

    @Nonnull
    AmazonKinesisFirehose createKinesisFirehoseClient() {
        return AWSUtil.createKinesisFirehoseClientFromConfiguration(config, credentialsProvider);
    }

    @Nonnull
    IProducer<UserRecordResult, Record> createFirehoseProducer() {
        return new FirehoseProducer<>(defaultDeliveryStream, firehoseClient, config);
    }

    @Override
    public void invoke(final OUT value, final Context context) throws Exception {
        Validate.notNull(value);
        ByteBuffer serializedValue = schema.serialize(value);

        Validate.validState((firehoseProducer != null && !firehoseProducer.isDestroyed()),
                "Firehose producer has been destroyed");
        Validate.validState(firehoseClient != null, "Kinesis Firehose client has been closed");

        propagateAsyncExceptions();

        firehoseProducer
                .addUserRecord(new Record().withData(serializedValue))
                .handleAsync((record, throwable) -> {
                    if (throwable != null) {
                        final String msg = "An error has occurred trying to write a record.";
                        if (failOnError) {
                            lastThrownException = throwable;
                        } else {
                            LOGGER.warn(msg, throwable);
                        }
                    }

                    if (record != null && !record.isSuccessful()) {
                        final String msg = "Record could not be successfully sent.";
                        if (failOnError && lastThrownException == null) {
                            lastThrownException = new RecordCouldNotBeSentException(msg, record.getException());
                        } else {
                            LOGGER.warn(msg, record.getException());
                        }
                    }

                    return null;
                });
    }

    @Override
    public void snapshotState(final FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //Propagates asynchronously wherever exception that might happened previously.
        propagateAsyncExceptions();

        //Forces the Firehose producer to flush the buffer.
        LOGGER.debug("Outstanding records before snapshot: {}", firehoseProducer.getOutstandingRecordsCount());
        flushSync();
        LOGGER.debug("Outstanding records after snapshot: {}", firehoseProducer.getOutstandingRecordsCount());
        if (firehoseProducer.getOutstandingRecordsCount() > 0) {
            throw new IllegalStateException("An error has occurred trying to flush the buffer synchronously.");
        }

        // If the flush produced any exceptions, we should propagates it also and fail the checkpoint.
        propagateAsyncExceptions();
    }

    @Override
    public void initializeState(final FunctionInitializationContext functionInitializationContext) throws Exception {
        //No Op
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
            propagateAsyncExceptions();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw ex;
        } finally {
            flushSync();
            firehoseProducer.destroy();
            if (firehoseClient != null) {
                LOGGER.debug("Shutting down Kinesis Firehose client...");
                firehoseClient.shutdown();
            }
        }
    }

    private void propagateAsyncExceptions() throws Exception {
        if (lastThrownException == null) {
            return;
        }

        final String msg = "An exception has been thrown while trying to process a record";
        if (failOnError) {
            throw new FlinkKinesisFirehoseException(msg, lastThrownException);
        } else {
            LOGGER.warn(msg, lastThrownException);
            lastThrownException = null;
        }
    }

    /**
     * This method waits until the buffer is flushed, an error has occurred or the thread was interrupted.
     */
    private void flushSync() {
        while (firehoseProducer.getOutstandingRecordsCount() > 0 && !firehoseProducer.isFlushFailed()) {
            firehoseProducer.flush();
            try {
                LOGGER.debug("Number of outstanding records before going to sleep: {}", firehoseProducer.getOutstandingRecordsCount());
                Thread.sleep(500);
            } catch (InterruptedException ex) {
                LOGGER.warn("Flushing has been interrupted.");
                break;
            }
        }
    }
}
