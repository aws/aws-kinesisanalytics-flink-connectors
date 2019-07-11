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
import com.amazonaws.services.kinesisanalytics.flink.connectors.serialization.JsonSerializationSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Properties;

public class SimpleWordCount {

    private static final String SINK_NAME = "Flink Kinesis Firehose Sink";

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // get input data
        DataStream<String> text = env.fromElements(WordCountData.WORDS);

        DataStream<Tuple2<String, Integer>> counts =
            // normalize and split each line
            text.map(line -> line.toLowerCase().split("\\W+"))
                // convert split line in pairs (2-tuples) containing: (word,1)
                .flatMap(new FlatMapFunction<String[], Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String[] value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Arrays.stream(value)
                            .filter(t -> t.length() > 0)
                            .forEach(t -> out.collect(new Tuple2<>(t, 1)));
                    }
                })
                // group by the tuple field "0" and sum up tuple field "1"
                .keyBy(0)
                .sum(1);

        Properties configProps = new Properties();
        configProps.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
        configProps.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
        configProps.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");

        FlinkKinesisFirehoseProducer<Tuple2<String, Integer>> producer =
            new FlinkKinesisFirehoseProducer<>("firehose-delivery-stream", new JsonSerializationSchema<>(),
                configProps);

        counts.addSink(producer).name(SINK_NAME);
        env.execute();
    }
}
