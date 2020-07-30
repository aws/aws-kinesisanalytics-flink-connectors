# Changelog

## Release 2.0.0 (July 30th, 2020)
### Milestones
- [Milestone v2.0.0](https://github.com/aws/aws-kinesisanalytics-flink-connectors/milestone/2)
- [Milestone v2.0.0-RC1](https://github.com/aws/aws-kinesisanalytics-flink-connectors/milestone/1)

### New Features
* Adding Assume Role credential provider
    ([PR#8](https://github.com/aws/aws-kinesisanalytics-flink-connectors/pull/8))  
  
### Bug Fixes
* Prevent Firehose Sink from exporting batches that exceed the maximum size of 
    [4MiB per call](https://docs.aws.amazon.com/firehose/latest/dev/limits.html).
    Limits maximum batch size to 1MiB in regions with a 1MiB/second throughput quota
    ([PR#9](https://github.com/aws/aws-kinesisanalytics-flink-connectors/pull/9))
  
* Fix `ProfileCredentialsProvider` when specifying a custom path to the configuration file
    ([PR#4](https://github.com/aws/aws-kinesisanalytics-flink-connectors/pull/4))

### Other Changes
* Update AWS SDK from version `1.11.379` to `1.11.803`
    ([PR#10](https://github.com/aws/aws-kinesisanalytics-flink-connectors/pull/10))

* Remove dependency on Guava
    ([PR#10](https://github.com/aws/aws-kinesisanalytics-flink-connectors/pull/10))
    
* Shaded and relocated AWS SDK dependency
    ([PR#7](https://github.com/aws/aws-kinesisanalytics-flink-connectors/pull/7))

### Migration Notes
* The following dependencies have been removed/relocated. 
    Any transitive references will no longer resolve in upstream projects:
    
    ```xml
    <!-- Guava -->
    <dependency>
        <groupId>com.google.guava</groupId>
        <artifactId>guava</artifactId>
        <version>${guava.version}</version>
    </dependency>

    <!-- AWS SDK -->
    <dependency>
        <groupId>com.amazonaws</groupId>
        <artifactId>aws-java-sdk-iam</artifactId>
        <version>${aws-sdk.version}</version>
    </dependency>
    <dependency>
        <groupId>com.amazonaws</groupId>
        <artifactId>aws-java-sdk-kinesis</artifactId>
        <version>${aws-sdk.version}</version>
    </dependency>
    ```
    