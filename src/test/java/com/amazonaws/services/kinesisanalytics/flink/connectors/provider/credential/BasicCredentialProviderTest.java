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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class BasicCredentialProviderTest {

    private BasicCredentialProvider basicCredentialProvider;

    @BeforeMethod
    public void setUp() {
        Properties properties = new Properties();
        properties.put(AWSConfigConstants.accessKeyId(), "ACCESS");
        properties.put(AWSConfigConstants.secretKey(), "SECRET");
        properties.put(AWSConfigConstants.AWS_REGION, "eu-west-2");

        basicCredentialProvider = new BasicCredentialProvider(properties);
    }

    @Test
    public void testGetAwsCredentialsProvider() {
        AWSCredentials credentials = basicCredentialProvider.getAwsCredentialsProvider().getCredentials();

        assertThat(credentials.getAWSAccessKeyId()).isEqualTo("ACCESS");
        assertThat(credentials.getAWSSecretKey()).isEqualTo("SECRET");
    }

    @Test
    public void testGetAwsCredentialsProviderSuppliesCredentialsAfterRefresh() {
        AWSCredentialsProvider provider = basicCredentialProvider.getAwsCredentialsProvider();
        provider.refresh();

        AWSCredentials credentials = provider.getCredentials();
        assertThat(credentials.getAWSAccessKeyId()).isEqualTo("ACCESS");
        assertThat(credentials.getAWSSecretKey()).isEqualTo("SECRET");
    }
}