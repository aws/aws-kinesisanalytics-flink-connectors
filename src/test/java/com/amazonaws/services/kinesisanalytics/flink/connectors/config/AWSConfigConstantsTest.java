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

package com.amazonaws.services.kinesisanalytics.flink.connectors.config;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class AWSConfigConstantsTest {

    @Test
    public void testAccessKeyId() {
        assertThat(AWSConfigConstants.accessKeyId("prefix")).isEqualTo("prefix.basic.aws_access_key_id");
    }

    @Test
    public void testAccessKeyId_null() {
        assertThat(AWSConfigConstants.accessKeyId(null)).isEqualTo("aws_access_key_id");
    }

    @Test
    public void testAccessKeyId_empty() {
        assertThat(AWSConfigConstants.accessKeyId("")).isEqualTo("aws_access_key_id");
    }

    @Test
    public void testAccessKeyId_noPrefix() {
        assertThat(AWSConfigConstants.accessKeyId()).isEqualTo("aws_access_key_id");
    }

    @Test
    public void testSecretKey() {
        assertThat(AWSConfigConstants.secretKey("prefix")).isEqualTo("prefix.basic.aws_secret_access_key");
    }

    @Test
    public void testSecretKey_null() {
        assertThat(AWSConfigConstants.secretKey(null)).isEqualTo("aws_secret_access_key");
    }

    @Test
    public void testSecretKey_empty() {
        assertThat(AWSConfigConstants.secretKey("")).isEqualTo("aws_secret_access_key");
    }

    @Test
    public void testSecretKey_noPrefix() {
        assertThat(AWSConfigConstants.secretKey()).isEqualTo("aws_secret_access_key");
    }

    @Test
    public void testProfilePath() {
        assertThat(AWSConfigConstants.profilePath("prefix")).isEqualTo("prefix.profile.path");
    }

    @Test
    public void testProfilePath_empty() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> AWSConfigConstants.profilePath(""));
    }

    @Test
    public void testProfileName() {
        assertThat(AWSConfigConstants.profileName("prefix")).isEqualTo("prefix.profile.name");
    }

    @Test
    public void testProfileName_empty() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> AWSConfigConstants.profileName(""));
    }

    @Test
    public void testRoleArn() {
        assertThat(AWSConfigConstants.roleArn("prefix")).isEqualTo("prefix.role.arn");
    }

    @Test
    public void testRoleArn_empty() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> AWSConfigConstants.roleArn(""));
    }

    @Test
    public void testRoleSessionName() {
        assertThat(AWSConfigConstants.roleSessionName("prefix")).isEqualTo("prefix.role.sessionName");
    }

    @Test
    public void testRoleSessionName_empty() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> AWSConfigConstants.roleSessionName(""));
    }

    @Test
    public void testExternalId() {
        assertThat(AWSConfigConstants.externalId("prefix")).isEqualTo("prefix.role.externalId");
    }

    @Test
    public void testExternalId_empty() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> AWSConfigConstants.externalId(""));
    }

    @Test
    public void testRoleCredentialsProvider() {
        assertThat(AWSConfigConstants.roleCredentialsProvider("prefix")).isEqualTo("prefix.role.provider");
    }

    @Test
    public void testRoleCredentialsProvider_empty() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> AWSConfigConstants.roleCredentialsProvider(""));
    }
}