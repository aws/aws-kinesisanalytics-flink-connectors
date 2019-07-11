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

package com.amazonaws.services.kinesisanalytics.flink.connectors.firehose.examples;

import com.amazonaws.services.kinesisanalytics.flink.connectors.config.AWSConfigConstants;
import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class SimpleStreamString {

    private static final String SINK_NAME = "Flink Kinesis Firehose Sink";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> simpleStringStream = env.addSource(new EventsGenerator());

        Properties configProps = new Properties();
        configProps.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
        configProps.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
        configProps.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");

        FlinkKinesisFirehoseProducer<String> producer =
            new FlinkKinesisFirehoseProducer<>("firehose-delivery-stream-name", new SimpleStringSchema(),
                configProps);

        simpleStringStream.addSink(producer).name(SINK_NAME);
        env.execute();
    }

    /**
     * Data generator that creates strings starting with a sequence number followed by a dash and 12 random characters.
     */
    public static class EventsGenerator implements SourceFunction<String> {
        private boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            long seq = 0;
            while (running) {
                Thread.sleep(10);
                ctx.collect((seq++) + "-" + RandomStringUtils.randomAlphabetic(12));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
