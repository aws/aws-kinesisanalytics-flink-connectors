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
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants;
import com.amazonaws.services.kinesisanalytics.flink.connectors.provider.credential.factory.CredentialProviderFactory;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;

import javax.annotation.Nonnull;
import java.util.Properties;

import static com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.getCredentialProviderType;
import static com.amazonaws.services.kinesisanalytics.flink.connectors.util.AWSUtil.validateAssumeRoleCredentialsProvider;

public class AssumeRoleCredentialsProvider extends CredentialProvider {

    public AssumeRoleCredentialsProvider(final Properties properties, final String providerKey) {
        super(validateAssumeRoleCredentialsProvider(properties, providerKey), providerKey);
    }

    public AssumeRoleCredentialsProvider(final Properties properties) {
        this(properties, AWS_CREDENTIALS_PROVIDER);
    }

    @Override
    public AWSCredentialsProvider getAwsCredentialsProvider() {
        final String baseCredentialsProviderKey = AWSConfigConstants.roleCredentialsProvider(providerKey);
        final AWSConfigConstants.CredentialProviderType baseCredentialsProviderType = getCredentialProviderType(properties, baseCredentialsProviderKey);
        final CredentialProvider baseCredentialsProvider =
                CredentialProviderFactory.newCredentialProvider(baseCredentialsProviderType, properties, baseCredentialsProviderKey);
        final AWSSecurityTokenService baseCredentials = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(baseCredentialsProvider.getAwsCredentialsProvider())
                .withRegion(properties.getProperty(AWSConfigConstants.AWS_REGION))
                .build();

        return createAwsCredentialsProvider(
                properties.getProperty(AWSConfigConstants.roleArn(providerKey)),
                properties.getProperty(AWSConfigConstants.roleSessionName(providerKey)),
                properties.getProperty(AWSConfigConstants.externalId(providerKey)),
                baseCredentials);
    }

    AWSCredentialsProvider createAwsCredentialsProvider(@Nonnull String roleArn,
                                                        @Nonnull String roleSessionName,
                                                        @Nonnull String externalId,
                                                        @Nonnull AWSSecurityTokenService securityTokenService) {
        return new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, roleSessionName)
                .withExternalId(externalId)
                .withStsClient(securityTokenService)
                .build();
    }
}
