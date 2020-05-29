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
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.CredentialProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_KINESIS_FIREHOSE_ENDPOINT;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_KINESIS_FIREHOSE_ENDPOINT_SIGNING_REGION;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_REGION;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.accessKeyId;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.profileName;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.roleArn;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.roleSessionName;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.secretKey;

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

    public static Properties validateBasicProviderConfiguration(final Properties configProps, final String providerKey) {
        validateConfiguration(configProps);

        Validate.isTrue(configProps.containsKey(accessKeyId(providerKey)),
            "AWS access key must be specified with credential provider BASIC.");
        Validate.isTrue(configProps.containsKey(secretKey(providerKey)),
            "AWS secret key must be specified with credential provider BASIC.");

        return configProps;
    }

    public static Properties validateBasicProviderConfiguration(final Properties configProps) {
        return validateBasicProviderConfiguration(configProps, null);
    }

    public static boolean containsBasicProperties(final Properties configProps, final String providerKey) {
        Validate.notNull(configProps);
        return configProps.containsKey(accessKeyId(providerKey)) && configProps.containsKey(secretKey(providerKey));
    }

    public static AWSConfigConstants.CredentialProviderType getCredentialProviderType(final Properties configProps,
                                                                                      final String providerKey) {
        if (providerKey == null || !configProps.containsKey(providerKey)) {
               return containsBasicProperties(configProps, providerKey) ?
                       AWSConfigConstants.CredentialProviderType.BASIC : AWSConfigConstants.CredentialProviderType.AUTO;
        }

        final String providerTypeString = configProps.getProperty(providerKey);
        if (StringUtils.isEmpty(providerTypeString)) {
            return AWSConfigConstants.CredentialProviderType.AUTO;
        }

        try {
            return AWSConfigConstants.CredentialProviderType.valueOf(providerTypeString);
        } catch (InvalidArgumentException e) {
            return AWSConfigConstants.CredentialProviderType.AUTO;
        }
    }

    public static Properties validateProfileProviderConfiguration(final Properties configProps, final String providerKey) {
        validateConfiguration(configProps);
        Validate.notBlank(providerKey);

        Validate.isTrue(configProps.containsKey(profileName(providerKey)),
            "AWS profile name should be specified with credential provider PROFILE.");

        return configProps;
    }

    public static Properties validateProfileProviderConfiguration(final Properties configProps) {
        return validateProfileProviderConfiguration(configProps, AWS_CREDENTIALS_PROVIDER);
    }

    public static Properties validateAssumeRoleCredentialsProvider(final Properties configProps, final String providerKey) {
        validateConfiguration(configProps);

        Validate.isTrue(configProps.containsKey(roleArn(providerKey)),
                "AWS role arn to be assumed must be provided with credential provider type ASSUME_ROLE");
        Validate.isTrue(configProps.containsKey(roleSessionName(providerKey)),
                "AWS role session name must be provided with credential provider type ASSUME_ROLE");

        return configProps;
    }

    public static Properties validateAssumeRoleCredentialsProvider(final Properties configProps) {
        return validateAssumeRoleCredentialsProvider(configProps, AWS_CREDENTIALS_PROVIDER);
    }
}
