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

import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.BasicCredentialProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_PROFILE_NAME;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_REGION;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.ASSUME_ROLE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.AUTO;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.BASIC;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.createKinesisFirehoseClientFromConfiguration;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.getCredentialProviderType;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.validateAssumeRoleCredentialsProvider;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.validateBasicProviderConfiguration;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.validateConfiguration;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.validateProfileProviderConfiguration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class AWSUtilTest {

    private Properties configProps;

    @BeforeMethod
    public void setUp() {
        configProps = new Properties();
        configProps.setProperty(AWS_ACCESS_KEY_ID, "DUMMY");
        configProps.setProperty(AWS_SECRET_ACCESS_KEY, "DUMMY-SECRET");
        configProps.setProperty(AWS_PROFILE_NAME, "Test");
        configProps.setProperty(AWS_REGION, "us-east-1");
    }

    @Test
    public void testCreateKinesisFirehoseClientFromConfigurationWithNullConfiguration() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> createKinesisFirehoseClientFromConfiguration(null, new BasicCredentialProvider(configProps)))
                .withMessageContaining("Configuration properties cannot be null");
    }

    @Test
    public void testCreateKinesisFirehoseClientFromConfigurationWithNullCredentialProvider() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> createKinesisFirehoseClientFromConfiguration(configProps, null))
                .withMessageContaining("Credential Provider cannot be null");
    }

    @Test
    public void testCreateKinesisFirehoseClientFromConfigurationHappyCase() {
        AmazonKinesisFirehose firehoseClient = createKinesisFirehoseClientFromConfiguration(configProps,
            new BasicCredentialProvider(configProps));

        assertThat(firehoseClient).isNotNull();
    }

    @Test
    public void testValidateConfigurationWithNullConfiguration() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> validateConfiguration(null))
                .withMessageContaining("Configuration properties cannot be null");
    }

    @Test
    public void testValidateConfigurationWithNoRegionOrFirehoseEndpoint() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> validateConfiguration(new Properties()))
                .withMessageContaining("Either AWS region should be specified or AWS Firehose endpoint and endpoint signing region");
    }

    @Test
    public void testValidateConfigurationHappyCase() {
        Properties config = validateConfiguration(configProps);
        assertThat(configProps).isEqualTo(config);
    }

    @Test
    public void testValidateBasicConfigurationHappyCase() {
        Properties config = validateBasicProviderConfiguration(configProps);
        assertThat(configProps).isEqualTo(config);
    }

    @Test
    public void testValidateBasicConfigurationWithNullConfiguration() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> validateBasicProviderConfiguration(null))
                .withMessageContaining("Configuration properties cannot be null");
    }

    @Test
    public void testValidateBasicConfigurationWithNoAwsAccessKeyId() {
        configProps.remove(AWS_ACCESS_KEY_ID);

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> validateBasicProviderConfiguration(configProps))
                .withMessageContaining("AWS access key must be specified with credential provider BASIC");
    }

    @Test
    public void testValidateBasicConfigurationWithNoAwsSecretKeyId() {
        configProps.remove(AWS_SECRET_ACCESS_KEY);

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> validateBasicProviderConfiguration(configProps))
                .withMessageContaining("AWS secret key must be specified with credential provider BASIC");
    }

    @Test
    public void testValidateProfileProviderConfigurationWithNullConfiguration() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> validateProfileProviderConfiguration(null))
                .withMessageContaining("Configuration properties cannot be null");
    }

    @Test
    public void testValidateProfileProviderConfigurationHappyCase() {
        Properties config = validateProfileProviderConfiguration(configProps);
        assertThat(configProps).isEqualTo(config);
    }

    @Test
    public void testValidateProfileProviderConfigurationWithNoProfileName() {
        configProps.remove(AWS_PROFILE_NAME);

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> validateProfileProviderConfiguration(configProps))
                .withMessageContaining("AWS profile name should be specified with credential provider PROFILE");
    }

    @Test
    public void testValidateAssumeRoleProviderConfigurationHappyCase() {
        Properties properties = buildAssumeRoleProperties();
        assertThat(validateAssumeRoleCredentialsProvider(properties)).isEqualTo(properties);
    }

    @Test
    public void testValidateAssumeRoleProviderConfigurationWithNoRoleArn() {
        Properties properties = buildAssumeRoleProperties();
        properties.remove(AWSConfigConstants.roleArn(AWS_CREDENTIALS_PROVIDER));

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> validateAssumeRoleCredentialsProvider(properties))
                .withMessageContaining("AWS role arn to be assumed must be provided with credential provider type ASSUME_ROLE");
    }

    @Test
    public void testValidateAssumeRoleProviderConfigurationWithNoRoleSessionName() {
        Properties properties = buildAssumeRoleProperties();
        properties.remove(AWSConfigConstants.roleSessionName(AWS_CREDENTIALS_PROVIDER));

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> validateAssumeRoleCredentialsProvider(properties))
                .withMessageContaining("AWS role session name must be provided with credential provider type ASSUME_ROLE");
    }

    @Test
    public void testValidateAssumeRoleProviderConfigurationWithNullConfiguration() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> validateAssumeRoleCredentialsProvider(null))
                .withMessageContaining("Configuration properties cannot be null");
    }

    @Nonnull
    private Properties buildAssumeRoleProperties() {
        Properties properties = new Properties();
        properties.putAll(configProps);
        properties.put(AWSConfigConstants.roleArn(AWS_CREDENTIALS_PROVIDER), "arn-1234567812345678");
        properties.put(AWSConfigConstants.roleSessionName(AWS_CREDENTIALS_PROVIDER), "session-name");
        return properties;
    }

    @Test
    public void testGetCredentialProviderTypeIsAutoNullProviderKey() {
        assertThat(getCredentialProviderType(new Properties(), null)).isEqualTo(AUTO);
    }

    @Test
    public void testGetCredentialProviderTypeIsAutoWithProviderKeyMismatch() {
        assertThat(getCredentialProviderType(configProps, "missing-key")).isEqualTo(AUTO);
    }

    @Test
    public void testGetCredentialProviderTypeIsAutoMissingAccessKey() {
        configProps.remove(AWS_ACCESS_KEY_ID);

        assertThat(getCredentialProviderType(configProps, null)).isEqualTo(AUTO);
    }

    @Test
    public void testGetCredentialProviderTypeIsAutoMissingSecretKey() {
        configProps.remove(AWS_SECRET_ACCESS_KEY);

        assertThat(getCredentialProviderType(configProps, null)).isEqualTo(AUTO);
    }

    @Test
    public void testGetCredentialProviderTypeIsBasic() {
        assertThat(getCredentialProviderType(configProps, null)).isEqualTo(BASIC);
    }

    @Test
    public void testGetCredentialProviderTypeIsAutoWithEmptyProviderKey() {
        configProps.setProperty("key", "");

        assertThat(getCredentialProviderType(configProps, "key")).isEqualTo(AUTO);
    }

    @Test
    public void testGetCredentialProviderTypeIsAutoWithBadConfiguration() {
        configProps.setProperty("key", "Bad");

        assertThat(getCredentialProviderType(configProps, "key")).isEqualTo(AUTO);
    }

    @Test
    public void testGetCredentialProviderTypeIsParsedFromProviderKey() {
        configProps.setProperty("key", "ASSUME_ROLE");

        assertThat(getCredentialProviderType(configProps, "key")).isEqualTo(ASSUME_ROLE);
    }
}
