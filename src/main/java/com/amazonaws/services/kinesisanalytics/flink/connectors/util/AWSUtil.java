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

package com.amazonaws.services.kinesisanalytics.flink.connectors.util;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.CredentialProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import org.apache.commons.lang3.Validate;

import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_KINESIS_FIREHOSE_ENDPOINT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_KINESIS_FIREHOSE_ENDPOINT_SIGNING_REGION;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_PROFILE_NAME;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_REGION;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;

public final class AWSUtil {

    private AWSUtil() {

    }

    public static AmazonKinesisFirehose createKinesisFirehoseClientFromConfiguration(final Properties configProps,
                                                                                     final CredentialProvider credentialsProvider) {
        validateConfiguration(configProps);
        Validate.notNull(credentialsProvider, "Credential Provider cannot be null.");

        AmazonKinesisFirehoseClientBuilder firehoseClientBuilder = AmazonKinesisFirehoseClientBuilder
            .standard()
            .withCredentials(credentialsProvider.getAwsCredentialsProvider());

        final String region = configProps.getProperty(AWS_REGION, null);

        final String firehoseEndpoint = configProps.getProperty(
            AWS_KINESIS_FIREHOSE_ENDPOINT, null);

        final String firehoseEndpointSigningRegion = configProps.getProperty(
            AWS_KINESIS_FIREHOSE_ENDPOINT_SIGNING_REGION, null);

        firehoseClientBuilder = (region != null) ? firehoseClientBuilder.withRegion(Regions.fromName(region))
            : firehoseClientBuilder.withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(firehoseEndpoint, firehoseEndpointSigningRegion));

        return firehoseClientBuilder.build();
    }

    public static Properties validateConfiguration(final Properties configProps) {
        Validate.notNull(configProps, "Configuration properties cannot be null.");

        if (!configProps.containsKey(AWS_REGION) ^ (configProps.containsKey(AWS_KINESIS_FIREHOSE_ENDPOINT) &&
            configProps.containsKey(AWS_KINESIS_FIREHOSE_ENDPOINT_SIGNING_REGION))) {

            throw new IllegalArgumentException(
                "Either AWS region should be specified or AWS Firehose endpoint and endpoint signing region.");
        }

        return configProps;
    }

    public static Properties validateBasicProviderConfiguration(final Properties configProps) {
        validateConfiguration(configProps);

        Validate.isTrue(configProps.containsKey(AWS_ACCESS_KEY_ID),
            "AWS access key must be specified with credential provider BASIC.");
        Validate.isTrue(configProps.containsKey(AWS_SECRET_ACCESS_KEY),
            "AWS secret key must be specified with credential provider BASIC.");

        return configProps;
    }

    public static boolean containsBasicProperties(final Properties configProps) {
        Validate.notNull(configProps);
        return configProps.containsKey(AWS_ACCESS_KEY_ID) &&
            configProps.containsKey(AWS_SECRET_ACCESS_KEY);
    }

    public static Properties validateProfileProviderConfiguration(final Properties configProps) {
        validateConfiguration(configProps);

        Validate.isTrue(configProps.containsKey(AWS_PROFILE_NAME),
            "AWS profile name should be specified with credential provider PROFILE.");

        return configProps;
    }
}
