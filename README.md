# Amazon Kinesis Flink Connectors

This repository contains various Apache Flink connectors to connect to [AWS Kinesis][kinesis] data sources and sinks. 

## Amazon Kinesis Data Firehose Producer for Apache Flink
This Producer allows Flink applications to push directly to [Kinesis Firehose][firehose].
- [AWS Documentation][firehose-documentation]
- [Issues][issues]

### Quickstart
Configure and instantiate a `FlinkKinesisFirehoseProducer`:

```java
Properties config = new Properties();
outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);

FlinkKinesisFirehoseProducer<String> sink = new FlinkKinesisFirehoseProducer<>(streamName, new SimpleStringSchema(), config);
```

### Getting Started
Follow the [example instructions][example] to create an end to end application:
- Write data into a [Kinesis Data Stream][kds]
- Process the streaming data using [Kinesis Data Analytics][kda]
- Write the results to a [Kinesis Firehose][firehose] using the `FlinkKinesisFirehoseProducer`
- Store the data in an [S3 Bucket][s3]

### Building from Source
1. You will need to install Java 1.8+ and Maven
1. Clone the repository from Github
1. Build using Maven from the project root directory: 
    1. `$ mvn clean install`

### Flink Version Matrix
Flink maintain backwards compatibility for the Sink interface used by the Firehose Producer. 
This project is compatible with Flink 1.x, there is no guarantee it will support Flink 2.x should it release in the future. 

Connector Version | Flink Version | Release Date
----------------- | ------------- | ------------
2.1.0 | 1.x | Feb, 2023
2.0.0 | 1.x | Jul, 2020
1.1.0 | 1.x | Dec, 2019
1.0.1 | 1.x | Dec, 2018
1.0.0 | 1.x | Dec, 2018

## License

This library is licensed under the Apache 2.0 License. 

[kinesis]: https://aws.amazon.com/kinesis
[firehose]: https://aws.amazon.com/kinesis/data-firehose/
[kds]: https://aws.amazon.com/kinesis/data-streams/
[kda]: https://aws.amazon.com/kinesis/data-analytics/
[s3]: https://aws.amazon.com/s3/
[firehose-documentation]: https://docs.aws.amazon.com/kinesisanalytics/latest/java/how-sinks.html#sinks-firehose-create
[issues]: https://github.com/aws/aws-kinesisanalytics-flink-connectors/issues
[example]: https://docs.aws.amazon.com/kinesisanalytics/latest/java/get-started-exercise-fh.html