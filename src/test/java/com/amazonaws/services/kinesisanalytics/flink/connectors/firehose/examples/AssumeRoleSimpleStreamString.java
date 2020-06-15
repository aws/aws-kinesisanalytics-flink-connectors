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

package com.amazonaws.services.kinesisanalytics.flink.connectors.firehose.examples;

import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants;
import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.ASSUME_ROLE;

/**
 * This example application streams dummy data to the specified Firehose using Assume Role authentication mechanism.
 * See https://docs.aws.amazon.com/kinesisanalytics/latest/java/examples-cross.html for more information.
 */
public class AssumeRoleSimpleStreamString {

    private static final String SINK_NAME = "Flink Kinesis Firehose Sink";
    private static final String STREAM_NAME = "<replace-with-your-stream>";
    private static final String ROLE_ARN = "<replace-with-your-role-arn>";
    private static final String ROLE_SESSION_NAME = "<replace-with-your-role-session-name>";
    private static final String REGION = "us-east-1";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> simpleStringStream = env.addSource(new SimpleStreamString.EventsGenerator());

        Properties configProps = new Properties();
        configProps.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, ASSUME_ROLE.name());
        configProps.setProperty(AWSConfigConstants.AWS_ROLE_ARN, ROLE_ARN);
        configProps.setProperty(AWSConfigConstants.AWS_ROLE_SESSION_NAME, ROLE_SESSION_NAME);
        configProps.setProperty(AWSConfigConstants.AWS_REGION, REGION);

        FlinkKinesisFirehoseProducer<String> producer =
                new FlinkKinesisFirehoseProducer<>(STREAM_NAME, new SimpleStringSchema(), configProps);

        simpleStringStream.addSink(producer).name(SINK_NAME);
        env.execute();
    }
}
