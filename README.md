# GCS Analytics Core
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/GoogleCloudPlatform/gcs-analytics-core/badge)](https://scorecard.dev/viewer/?uri=github.com/GoogleCloudPlatform/gcs-analytics-core)
[![codecov](https://codecov.io/gh/GoogleCloudPlatform/gcs-analytics-core/branch/main/graph/badge.svg?token=4yjIB0AAw4)](https://codecov.io/gh/GoogleCloudPlatform/gcs-analytics-core)

The GCS Analytics Core is a Java library designed to optimize and accelerate analytics workloads on Google Cloud Storage (GCS). It provides a common set of functionalities that can be shared across various data processing frameworks like Apache Spark, Trino, and others that use the Hadoop-compatible file system (GCS-Connector) or interact with Apache Iceberg tables through its GCSFileIO implementation.

This library addresses the performance discrepancies and configuration complexities that arise from having multiple
integration paths to GCS. By centralizing key optimizations, the GCS Analytics Core aims to provide a
consistently high-performance experience for all analytics workloads on GCS.
Key Features
- **Vectored I/O**: Improves read performance by fetching multiple data ranges in a single operation, significantly
reducing the number of calls to GCS.
- **Adaptive Range Reads**: Dynamically adjusts the read buffer size to minimize data transfer and wasted bandwidth,
particularly when reading small segments of large files.
- **Parquet Footer Caching and Prefetching**: Caches Parquet file footers in memory to avoid redundant reads and
accelerate query planning and execution.
- **Unified and Simplified Configuration**: Provides a single, optimized path to GCS, eliminating the need for users to
understand and tune complex configurations for different access methods.


## Current Status
Under development.

## Build instructions

To build the library:
```shell
./mvnw clean package
```

To verify the test coverage, run the following commands from main directory:
```shell
./mvnw -P coverage clean verify -Dmaven.javadoc.skip=true -Dsource.skip=true  -Dgpg.skip=true
```
The coverage report can be found in `coverage/target/site/jacoco-aggregate`.

To run integration tests:
```shell
gcloud auth application-default login
./mvnw -Pintegration-test clean verify
```
