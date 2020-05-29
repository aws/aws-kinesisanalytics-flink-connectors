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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.validateProfileProviderConfiguration;

public class ProfileCredentialProvider extends CredentialProvider {


    public ProfileCredentialProvider(final Properties properties, final String providerKey) {
        super(validateProfileProviderConfiguration(properties, providerKey), providerKey);
    }

    public ProfileCredentialProvider(final Properties properties) {
        this(properties, AWS_CREDENTIALS_PROVIDER);
    }

    @Override
    public AWSCredentialsProvider getAwsCredentialsProvider() {
        final String profileName = properties.getProperty(AWSConfigConstants.profileName(providerKey));
        final String profilePath = properties.getProperty(AWSConfigConstants.profilePath(providerKey));

        return StringUtils.isEmpty(profilePath) ? new ProfileCredentialsProvider(profileName) :
            new ProfileCredentialsProvider(profileName, profilePath);
    }
}
