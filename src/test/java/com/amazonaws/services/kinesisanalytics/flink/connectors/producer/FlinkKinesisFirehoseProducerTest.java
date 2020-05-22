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
import com.amazonaws.services.kinesisanalytics.flink.connectors.serialization.KinesisFirehoseSerializationSchema;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.util.concurrent.SettableFuture;
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

import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.producer.impl.FirehoseProducer.UserRecordResult;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.DEFAULT_DELIVERY_STREAM;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.DEFAULT_TEST_ERROR_MSG;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.getContext;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.getKinesisFirehoseSerializationSchema;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.getSerializationSchema;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.testutils.TestUtils.getStandardProperties;
import static org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class FlinkKinesisFirehoseProducerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkKinesisFirehoseProducerTest.class);

    private FlinkKinesisFirehoseProducer<String> flinkKinesisFirehoseProducer;
    private Context context;
    private Configuration properties = new Configuration();

    @Mock
    private AmazonKinesisFirehose kinesisFirehoseClient;

    @Mock
    private IProducer<UserRecordResult, Record> firehoseProducer;

    @BeforeMethod
    public void init() {
        MockitoAnnotations.initMocks(this);

        flinkKinesisFirehoseProducer = spy(new FlinkKinesisFirehoseProducer<>(DEFAULT_DELIVERY_STREAM,
                getKinesisFirehoseSerializationSchema(), getStandardProperties(), kinesisFirehoseClient, firehoseProducer));
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
                                                          final KinesisFirehoseSerializationSchema schema,
                                                          final Properties configProps,
                                                          final CredentialProviderType credentialType) {
        FlinkKinesisFirehoseProducer<String> firehoseProducer = (credentialType != null) ?
                new FlinkKinesisFirehoseProducer<String>(deliveryStream, schema, configProps, credentialType) :
                new FlinkKinesisFirehoseProducer<String>(deliveryStream, schema, configProps);
        assertNotNull(firehoseProducer);
    }

    @Test(dataProvider = "serializationSchemaProvider")
    public void testFlinkKinesisFirehoseProducerWithSerializationSchemaHappyCase(final String deliveryStream ,
                                                                                 final SerializationSchema schema,
                                                                                 final Properties configProps,
                                                                                 CredentialProviderType credentialType) {
        FlinkKinesisFirehoseProducer<String> firehoseProducer = (credentialType != null) ?
                new FlinkKinesisFirehoseProducer<String>(deliveryStream, schema, configProps, credentialType) :
                new FlinkKinesisFirehoseProducer<String>(deliveryStream, schema, configProps);
        assertNotNull(firehoseProducer);
    }

    /**
     * This test is responsible for testing rethrow in for an async error closing the sink (producer).
     * @throws Exception
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
     * @throws Exception
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

    /**
     * This test is responsible for testing async error, however should not rethrow in case of failures.
     * This is the default scenario for FlinkKinesisFirehoseProducer.
     * @throws Exception
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
     * @throws Exception
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
        assertEquals(ex.getMessage(), expectedErrorMsg);

        assertTrue((ex.getCause() != null && ex.getCause() instanceof RecordCouldNotBeBuffered));

        LOGGER.info(ex.getCause().getMessage());
        assertEquals(ex.getCause().getMessage(), DEFAULT_TEST_ERROR_MSG);
    }

    private SettableFuture getUserRecordResult(final boolean isFailedRecord, final boolean isSuccessful) {
        UserRecordResult recordResult = new UserRecordResult().setSuccessful(isSuccessful);
        SettableFuture errorResult = SettableFuture.create();

        if (isFailedRecord) {
            Throwable ex = new RecordCouldNotBeBuffered(DEFAULT_TEST_ERROR_MSG);
            recordResult.setException(ex);
            errorResult.setException(ex);
        }

        errorResult.set(recordResult);
        return errorResult;
    }

}
