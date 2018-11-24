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

/**
 * AWS Kinesis Firehose configuration constants
 */
public class AWSConfigConstants {

    public enum CredentialProviderType {

        /** Look for AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY into passed configuration */
        BASIC,

        /** Look for the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to create AWS credentials. */
        ENV_VARIABLES,

        /** Look for Java system properties aws.accessKeyId and aws.secretKey to create AWS credentials. */
        SYS_PROPERTIES,

        /** Use a AWS credentials profile file to create the AWS credentials. */
        PROFILE,

        /** A credentials provider chain will be used that searches for credentials in this order:
         * ENV_VARIABLES, SYS_PROPERTIES, PROFILE in the AWS instance metadata. **/
        AUTO
    }


    /** The AWS access key for provider type basic */
    public static final String AWS_ACCESS_KEY_ID = "aws_access_key_id";

    /** The AWS secret key for provider type basic */
    public static final String AWS_SECRET_ACCESS_KEY = "aws_secret_access_key";

    /** The AWS Kinesis Firehose region, if not specified defaults to us-east-1 */
    public static final String AWS_REGION = "aws.region";

    /** The Kinesis Firehose endpoint */
    public static final String AWS_KINESIS_FIREHOSE_ENDPOINT = "aws.kinesis.firehose.endpoint";

    public static final String AWS_KINESIS_FIREHOSE_ENDPOINT_SIGNING_REGION = "aws.kinesis.firehose.endpoint.signing.region";

    /** Optional configuration in case the provider is AwsProfileCredentialProvider */
    public static final String AWS_PROFILE_NAME = "aws.credentials.provider.profile.name";

    /** Optional configuration in case the provider is AwsProfileCredentialProvider */
    public static final String AWS_PROFILE_PATH = "aws.credentials.provider.profile.path";
}
