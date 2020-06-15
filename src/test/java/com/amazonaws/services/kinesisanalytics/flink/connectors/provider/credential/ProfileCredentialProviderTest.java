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
import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants;
import org.testng.annotations.Test;

import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_REGION;
import static org.assertj.core.api.Assertions.assertThat;

public class ProfileCredentialProviderTest {

    @Test
    public void testGetAwsCredentialsProvider() {
        Properties properties = new Properties();
        properties.put(AWS_REGION, "eu-west-2");
        properties.put(AWSConfigConstants.profileName(AWS_CREDENTIALS_PROVIDER), "default");
        properties.put(AWSConfigConstants.profilePath(AWS_CREDENTIALS_PROVIDER), "src/test/resources/profile");

        AWSCredentials credentials = new ProfileCredentialProvider(properties)
                .getAwsCredentialsProvider().getCredentials();

        assertThat(credentials.getAWSAccessKeyId()).isEqualTo("AKIAIOSFODNN7EXAMPLE");
        assertThat(credentials.getAWSSecretKey()).isEqualTo("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
    }
}