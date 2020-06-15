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

package com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential;

import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_PROFILE_NAME;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_REGION;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.ASSUME_ROLE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.AUTO;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.BASIC;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.ENV_VARIABLES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.PROFILE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.SYS_PROPERTIES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.factory.CredentialProviderFactory.newCredentialProvider;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class CredentialProviderFactoryTest {

    private Properties configProps;

    @BeforeMethod
    public void setUp() {
        configProps = new Properties();
        configProps.setProperty(AWS_REGION, "us-west-2");
    }

    @Test
    public void testBasicCredentialProviderHappyCase() {
        configProps.setProperty(AWS_ACCESS_KEY_ID, "accessKeyId");
        configProps.setProperty(AWS_SECRET_ACCESS_KEY, "secretAccessKey");
        CredentialProvider credentialProvider = newCredentialProvider(BASIC, configProps);
        assertThat(credentialProvider).isInstanceOf(BasicCredentialProvider.class);
    }

    @Test
    public void testBasicCredentialProviderWithNullProviderKey() {
        configProps.setProperty(AWS_ACCESS_KEY_ID, "accessKeyId");
        configProps.setProperty(AWS_SECRET_ACCESS_KEY, "secretAccessKey");
        CredentialProvider credentialProvider = newCredentialProvider(BASIC, configProps, null);
        assertThat(credentialProvider).isInstanceOf(BasicCredentialProvider.class);
    }

    @Test
    public void testBasicCredentialProviderInvalidConfigurationProperties() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> newCredentialProvider(BASIC, configProps))
                .withMessageContaining("AWS access key must be specified with credential provider BASIC.");
    }

    @Test
    public void testProfileCredentialProviderHappyCase() {
        configProps.setProperty(AWS_PROFILE_NAME, "TEST");
        CredentialProvider credentialProvider = newCredentialProvider(PROFILE, configProps);
        assertThat(credentialProvider).isInstanceOf(ProfileCredentialProvider.class);
    }

    @Test
    public void testProfileCredentialProviderInvalidConfigurationProperties() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> newCredentialProvider(PROFILE, configProps))
                .withMessageContaining("AWS profile name should be specified with credential provider PROFILE.");
    }

    @Test
    public void testEnvironmentCredentialProviderHappyCase() {
        CredentialProvider credentialProvider = newCredentialProvider(ENV_VARIABLES, configProps);
        assertThat(credentialProvider).isInstanceOf(EnvironmentCredentialProvider.class);
    }

    @Test
    public void testSystemCredentialProviderHappyCase() {
        CredentialProvider credentialProvider = newCredentialProvider(SYS_PROPERTIES, configProps);
        assertThat(credentialProvider).isInstanceOf(SystemCredentialProvider.class);
    }

    @Test
    public void testDefaultCredentialProviderHappyCase() {
        CredentialProvider credentialProvider = newCredentialProvider(AUTO, configProps);
        assertThat(credentialProvider).isInstanceOf(DefaultCredentialProvider.class);
    }

    @Test
    public void testCredentialProviderWithNullProvider() {
        CredentialProvider credentialProvider = newCredentialProvider(null, configProps);
        assertThat(credentialProvider).isInstanceOf(DefaultCredentialProvider.class);
    }

    @Test
    public void testAssumeRoleCredentialProviderHappyCase() {
        configProps.setProperty(AWSConfigConstants.roleArn(AWS_CREDENTIALS_PROVIDER), "arn-1234567812345678");
        configProps.setProperty(AWSConfigConstants.roleSessionName(AWS_CREDENTIALS_PROVIDER), "role-session");
        CredentialProvider credentialProvider = newCredentialProvider(ASSUME_ROLE, configProps);
        assertThat(credentialProvider).isInstanceOf(AssumeRoleCredentialsProvider.class);
    }

    @Test
    public void testAssumeRoleCredentialProviderInvalidConfigurationProperties() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> newCredentialProvider(ASSUME_ROLE, configProps))
                .withMessageContaining("AWS role arn to be assumed must be provided with credential provider type ASSUME_ROLE");
    }
}
