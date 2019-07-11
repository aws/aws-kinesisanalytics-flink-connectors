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

import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.BasicCredentialProvider;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_PROFILE_NAME;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_REGION;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.createKinesisFirehoseClientFromConfiguration;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.validateBasicProviderConfiguration;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.validateConfiguration;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.validateProfileProviderConfiguration;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class AWSUtilTest {

    private Properties configProps;

    @BeforeMethod
    public void init() {
        configProps = new Properties();
        configProps.setProperty(AWS_ACCESS_KEY_ID, "DUMMY");
        configProps.setProperty(AWS_SECRET_ACCESS_KEY, "DUMMY-SECRET");
        configProps.setProperty(AWS_PROFILE_NAME, "Test");
        configProps.setProperty(AWS_REGION, "us-east-1");
    }


    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateKinesisFirehoseClientFromConfigurationWithNullConfiguration() {
        createKinesisFirehoseClientFromConfiguration(null, new BasicCredentialProvider(configProps));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateKinesisFirehoseClientFromConfigurationWithNullCredentialProvider() {
        createKinesisFirehoseClientFromConfiguration(configProps, null);
    }

    @Test
    public void testCreateKinesisFirehoseClientFromConfigurationHappyCase() {
        AmazonKinesisFirehose firehoseClient =  createKinesisFirehoseClientFromConfiguration(configProps,
            new BasicCredentialProvider(configProps));
        Assert.assertNotNull(firehoseClient);
        Assert.assertTrue(firehoseClient instanceof AmazonKinesisFirehose);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testValidateConfigurationWithNullConfiguration() {
        validateConfiguration(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp =
        "Either AWS region should be specified or AWS Firehose endpoint and endpoint signing region.*")
    public void testValidateConfigurationWithNoRegionOrFirehoseEndpoint() {
        Properties config = new Properties();
        configProps.setProperty(AWS_ACCESS_KEY_ID, "DUMMY");
        configProps.setProperty(AWS_SECRET_ACCESS_KEY, "DUMMY-SECRET");
        validateConfiguration(config);
    }

    @Test
    public void testValidateConfigurationHappyCase() {
        Properties config = validateConfiguration(configProps);
        assertThat(configProps, is(config));
    }

    @Test
    public void testValidateBasicConfigurationHappyCase() {
        Properties config = validateBasicProviderConfiguration(configProps);
        assertThat(configProps, is(config));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testValidateBasicConfigurationWithNullConfiguration() {
        validateBasicProviderConfiguration(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "AWS access key must be specified with credential provider BASIC.*")
    public void testValidateBasicConfigurationWithNoAwsAccessKeyId() {
        configProps.remove(AWS_ACCESS_KEY_ID);
        validateBasicProviderConfiguration(configProps);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "AWS secret key must be specified with credential provider BASIC.*")
    public void testValidateBasicConfigurationWithNoAwsSecretKeyId() {
        configProps.remove(AWS_SECRET_ACCESS_KEY);
        validateBasicProviderConfiguration(configProps);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testValidateProfileProviderConfigurationWithNullConfiguration() {
        validateProfileProviderConfiguration(null);
    }

    @Test
    public void testValidateProfileProviderConfigurationHappyCase() {
        Properties config = validateProfileProviderConfiguration(configProps);
        assertThat(configProps, is(config));
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
    expectedExceptionsMessageRegExp = "AWS profile name should be specified with credential provider PROFILE.*")
    public void testValidateProfileProviderConfigurationWithNoProfileName() {
        configProps.remove(AWS_PROFILE_NAME);
        validateProfileProviderConfiguration(configProps);
    }
}
