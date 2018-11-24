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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_PROFILE_NAME;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_REGION;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.AUTO;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.BASIC;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.ENV_VARIABLES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.PROFILE;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType.SYS_PROPERTIES;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.factory.CredentialProviderFactory.newCredentialProvider;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class CredentialProviderFactoryTest {

    private Properties configProps;

    @BeforeMethod
    public void init() {
        configProps = new Properties();
        configProps.setProperty(AWS_REGION, "us-west-2");
    }

    @Test
    public void testBasicCredentialProviderHappyCase() {
        configProps.setProperty(AWS_ACCESS_KEY_ID, "accessKeyId");
        configProps.setProperty(AWS_SECRET_ACCESS_KEY, "secretAccessKey");
        CredentialProvider credentialProvider = newCredentialProvider(BASIC, configProps);
        assertNotNull(credentialProvider);
        assertTrue(credentialProvider instanceof BasicCredentialProvider);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp ="AWS access key must be specified with credential provider BASIC.*")
    public void testBasicCredentialProviderInvalidConfigurationProperties() {
        newCredentialProvider(BASIC, configProps);
    }

    @Test
    public void testProfileCredentialProviderHappyCase() {
        configProps.setProperty(AWS_PROFILE_NAME, "TEST");
        CredentialProvider credentialProvider = newCredentialProvider(PROFILE, configProps);
        assertNotNull(credentialProvider);
        assertTrue(credentialProvider instanceof ProfileCredentialProvider);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "AWS profile name should be specified with credential provider PROFILE.*")
    public void testProfileCredentialProviderInvalidConfigurationProperties() {
        newCredentialProvider(PROFILE, configProps);
    }

    @Test
    public void testEnvironmentCredentialProviderHappyCase() {
        CredentialProvider credentialProvider = newCredentialProvider(ENV_VARIABLES, configProps);
        assertNotNull(credentialProvider);
        assertTrue(credentialProvider instanceof EnvironmentCredentialProvider);
    }

    @Test
    public void testSystemCredentialProviderHappyCase() {
        CredentialProvider credentialProvider = newCredentialProvider(SYS_PROPERTIES, configProps);
        assertNotNull(credentialProvider);
        assertTrue(credentialProvider instanceof SystemCredentialProvider);
    }

    @Test
    public void testDefaultCredentialProviderHappyCase() {
        CredentialProvider credentialProvider = newCredentialProvider(AUTO, configProps);
        assertNotNull(credentialProvider);
        assertTrue(credentialProvider instanceof DefaultCredentialProvider);
    }

    @Test
    public void testCredentialProviderWithNullProvider() {
        CredentialProvider credentialProvider = newCredentialProvider(null, configProps);
        assertNotNull(credentialProvider);
        assertTrue(credentialProvider instanceof DefaultCredentialProvider);
    }
}
