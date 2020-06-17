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
import com.amazonaws.services.kinesisanalytics.flink.connectors.exception.RecordCouldNotBeBuffered;
import com.amazonaws.services.kinesisanalytics.flink.connectors.exception.RecordCouldNotBeSentException;
import com.amazonaws.services.kinesisanalytics.flink.connectors.serialization.KinesisFirehoseSerializationSchema;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.Record;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.producer.impl.FirehoseProducer.UserRecordResult;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.DEFAULT_DELIVERY_STREAM;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.DEFAULT_TEST_ERROR_MSG;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.getContext;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.getKinesisFirehoseSerializationSchema;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.getSerializationSchema;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.getStandardProperties;
import static org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class FlinkKinesisFirehoseProducerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKinesisFirehoseProducerTest.class);

    private FlinkKinesisFirehoseProducer<String> flinkKinesisFirehoseProducer;
    private Context<String> context;
    private final Configuration properties = new Configuration();

    @Mock
    private AmazonKinesisFirehose kinesisFirehoseClient;

    @Mock
    private IProducer<UserRecordResult, Record> firehoseProducer;

    @BeforeMethod
    public void init() {
        MockitoAnnotations.initMocks(this);

        flinkKinesisFirehoseProducer = createProducer();
        doReturn(firehoseProducer).when(flinkKinesisFirehoseProducer).createFirehoseProducer();
        doReturn(kinesisFirehoseClient).when(flinkKinesisFirehoseProducer).createKinesisFirehoseClient();

        context = getContext();
    }

    @DataProvider(name = "kinesisFirehoseSerializationProvider")
    public Object[][] kinesisFirehoseSerializationProvider() {

        return new Object[][]{
                {DEFAULT_DELIVERY_STREAM, getKinesisFirehoseSerializationSchema(), getStandardProperties(), null},
                {DEFAULT_DELIVERY_STREAM, getKinesisFirehoseSerializationSchema(), getStandardProperties(), CredentialProviderType.BASIC},
        };
    }

    @DataProvider(name = "serializationSchemaProvider")
    public Object[][] serializationSchemaProvider() {
        return new Object[][] {
                {DEFAULT_DELIVERY_STREAM, getSerializationSchema(), getStandardProperties(), null},
                {DEFAULT_DELIVERY_STREAM, getSerializationSchema(), getStandardProperties(), CredentialProviderType.BASIC}
        };
    }

    @Test(dataProvider = "kinesisFirehoseSerializationProvider")
    public void testFlinkKinesisFirehoseProducerHappyCase(final String deliveryStream,
                                                          final KinesisFirehoseSerializationSchema<String> schema,
                                                          final Properties configProps,
                                                          final CredentialProviderType credentialType) {
        FlinkKinesisFirehoseProducer<String> firehoseProducer = (credentialType != null) ?
                new FlinkKinesisFirehoseProducer<>(deliveryStream, schema, configProps, credentialType) :
                new FlinkKinesisFirehoseProducer<>(deliveryStream, schema, configProps);
        assertNotNull(firehoseProducer);
    }

    @Test(dataProvider = "serializationSchemaProvider")
    public void testFlinkKinesisFirehoseProducerWithSerializationSchemaHappyCase(final String deliveryStream ,
                                                                                 final SerializationSchema<String> schema,
                                                                                 final Properties configProps,
                                                                                 CredentialProviderType credentialType) {
        FlinkKinesisFirehoseProducer<String> firehoseProducer = (credentialType != null) ?
                new FlinkKinesisFirehoseProducer<>(deliveryStream, schema, configProps, credentialType) :
                new FlinkKinesisFirehoseProducer<>(deliveryStream, schema, configProps);

        assertNotNull(firehoseProducer);
    }

    /**
     * This test is responsible for testing rethrow in for an async error closing the sink (producer).
     */
    @Test
    public void testAsyncErrorRethrownOnClose() throws Exception {
        try {
            flinkKinesisFirehoseProducer.setFailOnError(true);
            when(firehoseProducer.addUserRecord(any(Record.class)))
                    .thenReturn(getUserRecordResult(true, false));

            flinkKinesisFirehoseProducer.open(properties);
            flinkKinesisFirehoseProducer.invoke("Test", context);
            flinkKinesisFirehoseProducer.close();

            LOGGER.warn("Should not reach this line");
            fail();
        } catch (FlinkKinesisFirehoseException ex) {
            LOGGER.info("Exception has been thrown inside testAsyncErrorRethrownOnClose");
            exceptionAssert(ex);

        } finally {
            verify(flinkKinesisFirehoseProducer, times(1)).open(properties);
            verify(flinkKinesisFirehoseProducer, times(1)).invoke("Test", context);
            verify(flinkKinesisFirehoseProducer, times(1)).close();
        }
    }

    /**
     * This test is responsible for testing an async error rethrow during invoke.
     */
    @Test
    public void testAsyncErrorRethrownOnInvoke() throws Exception {
        try {
            flinkKinesisFirehoseProducer.setFailOnError(true);
            when(firehoseProducer.addUserRecord(any(Record.class)))
                    .thenReturn(getUserRecordResult(true, false))
                    .thenReturn(getUserRecordResult(false, true));

            flinkKinesisFirehoseProducer.open(properties);
            flinkKinesisFirehoseProducer.invoke("Test", context);
            flinkKinesisFirehoseProducer.invoke("Test2", context);
            LOGGER.warn("Should not reach this line");
            fail();

        } catch (FlinkKinesisFirehoseException ex) {
            LOGGER.info("Exception has been thrown inside testAsyncErrorRethrownOnInvoke");
            exceptionAssert(ex);

        } finally {
            verify(flinkKinesisFirehoseProducer, times(1)).open(properties);
            verify(flinkKinesisFirehoseProducer, times(1)).invoke("Test", context);
            verify(flinkKinesisFirehoseProducer, times(1)).invoke("Test2", context);
            verify(flinkKinesisFirehoseProducer, never()).close();
        }
    }

    @Test
    public void testAsyncErrorRethrownWhenRecordFailedToSend() throws Exception {
        flinkKinesisFirehoseProducer.setFailOnError(true);

        UserRecordResult recordResult = new UserRecordResult();
        recordResult.setSuccessful(false);
        recordResult.setException(new RuntimeException("A bad thing has happened"));

        when(firehoseProducer.addUserRecord(any(Record.class)))
                .thenReturn(CompletableFuture.completedFuture(recordResult));

        flinkKinesisFirehoseProducer.open(properties);
        flinkKinesisFirehoseProducer.invoke("Test", context);

        assertThatExceptionOfType(FlinkKinesisFirehoseException.class)
                .isThrownBy(() -> flinkKinesisFirehoseProducer.close())
                .withMessageContaining("An exception has been thrown while trying to process a record")
                .withCauseInstanceOf(RecordCouldNotBeSentException.class)
                .withStackTraceContaining("A bad thing has happened");
    }

    /**
     * This test is responsible for testing async error, however should not rethrow in case of failures.
     * This is the default scenario for FlinkKinesisFirehoseProducer.
     */
    @Test
    public void testAsyncErrorNotRethrowOnInvoke() throws Exception {
        flinkKinesisFirehoseProducer.setFailOnError(false);

        when(firehoseProducer.addUserRecord(any(Record.class)))
                .thenReturn(getUserRecordResult(true, false))
                .thenReturn(getUserRecordResult(true, true));

        flinkKinesisFirehoseProducer.open(properties);
        flinkKinesisFirehoseProducer.invoke("Test", context);
        flinkKinesisFirehoseProducer.invoke("Test2", context);

        verify(flinkKinesisFirehoseProducer, times(1)).open(properties);
        verify(flinkKinesisFirehoseProducer, times(1)).invoke("Test", context);
        verify(flinkKinesisFirehoseProducer, times(1)).invoke("Test2", context);
        verify(flinkKinesisFirehoseProducer, never()).close();
    }

    @Test
    public void testFlinkKinesisFirehoseProducerHappyWorkflow() throws Exception {

        when(firehoseProducer.addUserRecord(any(Record.class)))
                .thenReturn(getUserRecordResult(false, true));

        flinkKinesisFirehoseProducer.open(properties);
        flinkKinesisFirehoseProducer.invoke("Test", context);
        flinkKinesisFirehoseProducer.close();

        verify(flinkKinesisFirehoseProducer, times(1)).open(properties);
        verify(flinkKinesisFirehoseProducer, times(1)).invoke("Test", context);
        verify(flinkKinesisFirehoseProducer, times(1)).close();
    }

    @Test
    public void testFlinkKinesisFirehoseProducerCloseAndFlushHappyWorkflow() throws Exception {

        when(firehoseProducer.addUserRecord(any(Record.class)))
                .thenReturn(getUserRecordResult(false, true));

        doNothing().when(firehoseProducer).flush();

        when(firehoseProducer.getOutstandingRecordsCount()).thenReturn(1).thenReturn(0);
        when(firehoseProducer.isFlushFailed()).thenReturn(false);

        flinkKinesisFirehoseProducer.open(properties);
        flinkKinesisFirehoseProducer.invoke("Test", context);
        flinkKinesisFirehoseProducer.close();

        verify(firehoseProducer, times(1)).flush();
    }

    @Test
    public void testFlinkKinesisFirehoseProducerTakeSnapshotHappyWorkflow() throws Exception {

        when(firehoseProducer.addUserRecord(any(Record.class)))
                .thenReturn(getUserRecordResult(false, true));

        doNothing().when(firehoseProducer).flush();

        when(firehoseProducer.getOutstandingRecordsCount()).thenReturn(1).thenReturn(1).thenReturn(1).thenReturn(0);
        when(firehoseProducer.isFlushFailed()).thenReturn(false);

        FunctionSnapshotContext functionContext = mock(FunctionSnapshotContext.class);
        flinkKinesisFirehoseProducer.open(properties);
        flinkKinesisFirehoseProducer.invoke("Test", context);
        flinkKinesisFirehoseProducer.snapshotState(functionContext);

        verify(firehoseProducer, times(1)).flush();
    }

    @Test(expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "An error has occurred trying to flush the buffer synchronously.*")
    public void testFlinkKinesisFirehoseProducerTakeSnapshotFailedFlush() throws Exception {

        when(firehoseProducer.addUserRecord(any(Record.class)))
                .thenReturn(getUserRecordResult(false, true));

        doNothing().when(firehoseProducer).flush();

        when(firehoseProducer.getOutstandingRecordsCount()).thenReturn(1).thenReturn(1);
        when(firehoseProducer.isFlushFailed()).thenReturn(false).thenReturn(true);

        FunctionSnapshotContext functionContext = mock(FunctionSnapshotContext.class);
        flinkKinesisFirehoseProducer.open(properties);
        flinkKinesisFirehoseProducer.invoke("Test", context);
        flinkKinesisFirehoseProducer.snapshotState(functionContext);

        fail("We should not reach here.");
    }

    /**
     * This test is responsible for testing a scenarion when there are exceptions to be thrown closing the sink (producer)
     * This is the default scenario for FlinkKinesisFirehoseProducer.
     */
    @Test
    public void testAsyncErrorNotRethrownOnClose() throws Exception {

        flinkKinesisFirehoseProducer.setFailOnError(false);

        when(firehoseProducer.addUserRecord(any(Record.class)))
                .thenReturn(getUserRecordResult(true, false))
                .thenReturn(getUserRecordResult(true, false));

        flinkKinesisFirehoseProducer.open(properties);
        flinkKinesisFirehoseProducer.invoke("Test", context);
        flinkKinesisFirehoseProducer.invoke("Test2", context);
        flinkKinesisFirehoseProducer.close();

        verify(flinkKinesisFirehoseProducer, times(1)).open(properties);
        verify(flinkKinesisFirehoseProducer, times(1)).invoke("Test", context);
        verify(flinkKinesisFirehoseProducer, times(1)).invoke("Test2", context);
        verify(flinkKinesisFirehoseProducer, times(1)).close();
    }

    private void exceptionAssert(FlinkKinesisFirehoseException ex) {
        final String expectedErrorMsg = "An exception has been thrown while trying to process a record";
        LOGGER.info(ex.getMessage());
        assertThat(ex.getMessage()).isEqualTo(expectedErrorMsg);

        assertThat(ex.getCause()).isInstanceOf(RecordCouldNotBeBuffered.class);

        LOGGER.info(ex.getCause().getMessage());
        assertThat(ex.getCause().getMessage()).isEqualTo(DEFAULT_TEST_ERROR_MSG);
    }

    @Nonnull
    private CompletableFuture<UserRecordResult> getUserRecordResult(final boolean isFailedRecord, final boolean isSuccessful) {
        UserRecordResult recordResult = new UserRecordResult().setSuccessful(isSuccessful);

        if (isFailedRecord) {
            CompletableFuture<UserRecordResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RecordCouldNotBeBuffered(DEFAULT_TEST_ERROR_MSG));
            return future;
        } else {
            return CompletableFuture.completedFuture(recordResult);
        }
    }

    @Nonnull
    private FlinkKinesisFirehoseProducer<String> createProducer() {
        return spy(new FlinkKinesisFirehoseProducer<>(DEFAULT_DELIVERY_STREAM,
                getKinesisFirehoseSerializationSchema(), getStandardProperties()));
    }
}
