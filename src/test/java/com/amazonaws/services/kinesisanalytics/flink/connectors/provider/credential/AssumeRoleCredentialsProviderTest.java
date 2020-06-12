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

import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class AssumeRoleCredentialsProviderTest {

    @Test
    public void testGetAwsCredentialsProvider() {
        Properties properties = new Properties();
        properties.put(AWSConfigConstants.AWS_REGION, "eu-west-2");
        properties.put(AWSConfigConstants.roleArn(AWS_CREDENTIALS_PROVIDER), "arn-1234567812345678");
        properties.put(AWSConfigConstants.roleSessionName(AWS_CREDENTIALS_PROVIDER), "session-name");
        properties.put(AWSConfigConstants.externalId(AWS_CREDENTIALS_PROVIDER), "external-id");

        STSAssumeRoleSessionCredentialsProvider expected = mock(STSAssumeRoleSessionCredentialsProvider.class);

        AssumeRoleCredentialsProvider provider = spy(new AssumeRoleCredentialsProvider(properties));
        doReturn(expected).when(provider).createAwsCredentialsProvider(any(), anyString(), anyString(), any());

        assertThat(provider.getAwsCredentialsProvider()).isEqualTo(expected);
        verify(provider).createAwsCredentialsProvider(eq("arn-1234567812345678"), eq("session-name"), eq("external-id"), any());
    }
}