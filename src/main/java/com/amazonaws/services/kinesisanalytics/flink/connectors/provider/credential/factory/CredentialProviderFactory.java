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

package com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.factory;

import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.CredentialProviderType;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.BasicCredentialProvider;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.CredentialProvider;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.DefaultCredentialProvider;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.EnvironmentCredentialProvider;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.ProfileCredentialProvider;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.SystemCredentialProvider;
import org.apache.commons.lang3.Validate;

import java.util.Properties;

public final class CredentialProviderFactory {

    private CredentialProviderFactory() {

    }

    public static CredentialProvider newCredentialProvider(final CredentialProviderType credentialProviderType,
                                                           final Properties awsConfigProps) {
        Validate.notNull(awsConfigProps, "AWS configuration properties cannot be null");

        final CredentialProviderType credentialType = (credentialProviderType == null) ?
            CredentialProviderType.AUTO : credentialProviderType;

        switch (credentialType) {
            case AUTO:
                return new DefaultCredentialProvider(awsConfigProps);
            case BASIC:
                return new BasicCredentialProvider(awsConfigProps);
            case PROFILE:
                return new ProfileCredentialProvider(awsConfigProps);
            case ENV_VARIABLES:
                return new EnvironmentCredentialProvider(awsConfigProps);
            case SYS_PROPERTIES:
                return new SystemCredentialProvider(awsConfigProps);
            default:
                return new DefaultCredentialProvider(awsConfigProps);
        }
    }
}
