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

package com.amazonaws.services.kinesisanalytics.flink.connectors.testutils;

import com.amazonaws.services.kinesisanalytics.flink.connectors.serialization.KinesisFirehoseSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_REGION;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;

public final class TestUtils {

    private TestUtils() {

    }

    public static final String DEFAULT_DELIVERY_STREAM = "test-stream";
    public static final String DEFAULT_TEST_ERROR_MSG = "Test exception";

    public static Properties getStandardProperties() {
        Properties config = new Properties();
        config.setProperty(AWS_REGION, "us-east-1");
        config.setProperty(AWS_ACCESS_KEY_ID, "accessKeyId");
        config.setProperty(AWS_SECRET_ACCESS_KEY, "awsSecretAccessKey");
        return config;
    }

    public static KinesisFirehoseSerializationSchema<String> getKinesisFirehoseSerializationSchema() {
        return (KinesisFirehoseSerializationSchema<String>) element -> ByteBuffer.wrap(element.getBytes(StandardCharsets.UTF_8));
    }

    public static SerializationSchema<String> getSerializationSchema() {
        return (SerializationSchema<String>) element ->
            ByteBuffer.wrap(element.getBytes(StandardCharsets.UTF_8)).array();
    }

    public static Context<String> getContext() {
        return new Context<String>() {
            @Override
            public long currentProcessingTime() {
                return System.currentTimeMillis();
            }

            @Override
            public long currentWatermark() {
                return 10L;
            }

            @Override
            public Long timestamp() {
                return System.currentTimeMillis();
            }
        };
    }
}
