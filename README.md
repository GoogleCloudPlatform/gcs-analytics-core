# GCS Analytics Core
[![GitHub release](https://img.shields.io/github/release/GoogleCloudPlatform/gcs-analytics-core.svg)](https://github.com/GoogleCloudPlatform/gcs-analytics-core/releases/latest)
[![GitHub release date](https://img.shields.io/github/release-date/GoogleCloudPlatform/gcs-analytics-core.svg)](https://github.com/GoogleCloudPlatform/gcs-analytics-core/releases/latest)
[![Maven Central](https://img.shields.io/maven-central/v/com.google.cloud.gcs.analytics/gcs-analytics-core.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.google.cloud.gcs.analytics%22%20AND%20a:%22gcs-analytics-core%22)
[![codecov](https://codecov.io/gh/GoogleCloudPlatform/gcs-analytics-core/branch/main/graph/badge.svg?token=4yjIB0AAw4)](https://codecov.io/gh/GoogleCloudPlatform/gcs-analytics-core)

The GCS Analytics Core is a Java library designed to optimize and accelerate analytics workloads on Google Cloud Storage (GCS). It provides a common set of functionalities that can be shared across various data processing frameworks like Apache Spark, Trino, and others that use the Hadoop-compatible file system (GCS-Connector) or interact with Apache Iceberg tables through its GCSFileIO implementation.

This library addresses the performance discrepancies and configuration complexities that arise from having multiple
integration paths to GCS. By centralizing key optimizations, the GCS Analytics Core aims to provide a
consistently high-performance experience for all analytics workloads on GCS.
Key Features
- **Vectored I/O**: Improves read performance by fetching multiple data ranges in a single operation, significantly
reducing the number of calls to GCS.
- **Parquet Footer Caching and Prefetching**: Caches Parquet file footers in memory to avoid redundant reads and
accelerate query planning and execution.
- **Unified and Simplified Configuration**: Provides a single, optimized path to GCS, eliminating the need for users to
understand and tune complex configurations for different access methods.

## Build instructions

To build the library:
```shell
./mvnw clean package
```

To verify the test coverage, run the following commands from main directory:
```shell
./mvnw -P coverage clean verify
```
The coverage report can be found in `coverage/target/site/jacoco-aggregate`.

To run integration tests:
```shell
gcloud auth application-default login

./mvnw -Pintegration-test verify \
  -Dgcs.integration.test.bucket=$BUCKET -Dgcs.integration.test.project-id=$PROJECT_ID \
  -Dgcs.integration.test.bucket.folder=$FOLDER_NAME
```

To run micro benchmarks:
```shell
./mvnw -Pjmh clean package

java -Dgcs.integration.test.bucket=$BUCKET_NAME -Dgcs.integration.test.project-id=$PROJECT_ID \
 -Dgcs.integration.test.bucket.folder=$FOLDER_NAME -jar core/target/benchmarks.jar
```

## Adding gcs-analytics-core to your build
Maven group ID is `com.google.cloud.gcs.analytics` and artifact ID is `gcs-analytics-core`.

To add a dependency on GCS Analytics Core using Maven, use the following:

```
<dependency>
  <groupId>com.google.cloud.gcs.analytics</groupId>
  <artifactId>gcs-analytics-core</artifactId>
  <version>x.y.x</version>
</dependency>
```
