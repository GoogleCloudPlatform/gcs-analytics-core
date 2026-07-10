# GCS Analytics Core

[![GitHub release](https://img.shields.io/github/release/GoogleCloudPlatform/gcs-analytics-core.svg)](https://github.com/GoogleCloudPlatform/gcs-analytics-core/releases/latest)
[![GitHub release date](https://img.shields.io/github/release-date/GoogleCloudPlatform/gcs-analytics-core.svg)](https://github.com/GoogleCloudPlatform/gcs-analytics-core/releases/latest)
[![Maven Central](https://img.shields.io/maven-central/v/com.google.cloud.gcs.analytics/gcs-analytics-core.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.google.cloud.gcs.analytics%22%20AND%20a:%22gcs-analytics-core%22)
[![codecov](https://codecov.io/gh/GoogleCloudPlatform/gcs-analytics-core/branch/main/graph/badge.svg?token=4yjIB0AAw4)](https://codecov.io/gh/GoogleCloudPlatform/gcs-analytics-core)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/GoogleCloudPlatform/gcs-analytics-core/badge)](https://api.securityscorecards.dev/projects/github.com/GoogleCloudPlatform/gcs-analytics-core)

## Introduction

The GCS Analytics Core is a Java library designed to optimize and accelerate analytics workloads on Google Cloud Storage (GCS). It provides a common set of functionalities and performance enhancements for Java applications interacting with GCS, particularly those using big data processing frameworks like Apache Spark, Trino, Apache Hive, and others that leverage the Google Cloud Storage connector for Hadoop or interact with Apache Iceberg tables through its GCSFileIO implementation.

This library aims to provide a consistently high-performance experience for all analytics workloads on GCS by centralizing key optimizations and simplifying configuration.

## Key Features

-   **Vectored I/O**: Improves read performance by fetching multiple data ranges in a single asynchronous operation, significantly reducing the number of round trips to GCS.
-   **Parquet Footer Prefetching & Caching**: Intelligently prefetches Parquet/ORC footers and caches them to avoid redundant network reads and accelerate query planning and execution.
-   **Small Object Prefetching & Caching**: Entirely prefetches and caches small objects below a configurable threshold, providing high-throughput sequential disk performance.
-   **Adaptive Read Optimization**: Dynamically heuristically detects file access patterns (`RANDOM` vs `SEQUENTIAL`) and uses in-place seek limits to tune network stream utilization in real-time.
-   **Comprehensive Telemetry**: Provides deep observability into cache hit/miss rates, I/O throughput, and stream durations. Seamlessly exports metrics via logs or directly to OpenTelemetry backends like Google Cloud Monitoring.
-   **Unified and Simplified Configuration**: Provides a single, optimized path to GCS, reducing the need for framework-specific tuning for GCS access.


## Architecture

The GCS Analytics Core library provides an optimized client layer `GcsFileSystem` and `GoogleCloudStorageInputStream`
which is a seekable input stream implementation that can be used by applications to interact with GCS. It sits between
the analytics frameworks and the underlying GCS Java library, intercepting calls to inject performance optimizations.

```mermaid
graph TD
    %% Top Layer: Query Engines
    subgraph Engines ["Query Engines"]
        QE["Spark, Trino, Hive, etc."]
    end

    %% Abstraction Layer
    subgraph Abstractions ["File System Abstractions"]
        direction LR
        FSA1["Hadoop GCS Connector<br>(GoogleHadoopFileSystem)"]
        FSA2["Apache Iceberg<br>(GCSFileIO)"]
    end

    %% Middle Layer: GCS Analytics Core
    subgraph Core ["GCS Analytics Core (Read-Optimized Layer)"]
        direction TB

        GFS_NODE["GcsFileSystem"]
        GCSIS_NODE["GoogleCloudStorageInputStream"]

        subgraph GFS ["File System Components"]
            GACO["GcsAnalyticsCoreOptions"]
            ACM["AnalyticsCacheManager"]
        end

        subgraph GCSIS ["Input Stream Features"]
            direction TB
            VIO["VectoredRead"]
            PFP["Parquet Footer Prefetch"]
            SOP["Small Object Prefetch"]
            ARR["Adaptive Range Read"]

            VIO ~~~ PFP ~~~ SOP ~~~ ARR
        end

        GACO -- "configures" --> GFS_NODE
        GFS_NODE -- "holds" --> ACM

        GCSIS_NODE -- "implements" --> VIO

        GFS_NODE -- "creates via open()" --> GCSIS_NODE

        PFP -. "reads/writes cache" .-> ACM
        SOP -. "reads/writes cache" .-> ACM
        VIO -. "reads cache" .-> ACM
    end

    %% Lower Layers
    Lib["Google Cloud Storage Java SDK"]
    GCS[("Google Cloud Storage (GCS)")]

    %% Relationships
    Engines --> Abstractions

    %% Scope Clarification: Core is Read-Only currently
    Abstractions -- "Read Path (open)" --> Core
    Abstractions -. "Future Write Path<br>(Under Development)" .-> Core
    Abstractions -- "Current Write Path & Namespace Ops<br>(Bypass directly to SDK)" --> Lib

    %% Flow downwards
    Core --> Lib
    Lib --> GCS

    %% Styling for visual clarity
    classDef engine fill:#f9f9f9,stroke:#333,stroke-width:1px;
    classDef abstraction fill:#e0f2f1,stroke:#2e7d32,stroke-width:1px;
    classDef core fill:#e3f2fd,stroke:#1565c0,stroke-width:2px;
    classDef feature fill:#ffffff,stroke:#1565c0,stroke-dasharray: 5 5;
    classDef lib fill:#fff3e0,stroke:#ef6c00,stroke-width:1px;
    classDef storage fill:#e8f5e9,stroke:#2e7d32,stroke-width:1px;

    class QE engine;
    class FSA1,FSA2 abstraction;
    class Core core;
    class VIO,PFP,SOP,ARR feature;
    class Lib lib;
    class GCS storage;
```

## Current Status

The library currently implements optimizations for read operations on columnar file formats (e.g., parquet) and small objects stored in GCS buckets.

## Getting Started

### Prerequisites

-   Java Development Kit (JDK) 11 or later.

### Documentation

*   **[Developer Guide](DEVELOPER_GUIDE.md)**: Detailed instructions on integrating `gcs-analytics-core` into your query engines or file system abstractions, including architecture deep-dives, installation instructions, and performance tuning best practices.
*   **[Configuration Guide](CONFIGURATION.md)**: A comprehensive list of all supported properties for tuning thread pools, caching, adaptive reads, and telemetry.

## Development
### Building from Source

To build the library:

```shell
./mvnw clean package
```

### Running Tests

To verify the test coverage, run the following commands from main directory:

```shell
./mvnw -P coverage clean verify
```

The coverage report can be found in `coverage/target/site/jacoco-aggregate`.

To run integration tests:

```shell
# Ensure you are authenticated
gcloud auth application-default login

# Run the tests
./mvnw -Pintegration-test verify \
  -Dgcs.integration.test.bucket=$BUCKET \
  -Dgcs.integration.test.project-id=$PROJECT_ID \
  -Dgcs.integration.test.bucket.folder=$FOLDER_NAME
```

Replace `$BUCKET`, `$PROJECT_ID`, and `$FOLDER_NAME` with your specific GCS bucket details.


## Micro Benchmarks

The project contains micro benchmark on top of parquet-java library. The benchmark creates a random parquet file with
customer schema from TPCDS benchmark and performs 2 operations :
1. Parse parquet footer.
2. Prase parquet footer and read parquet records.

To run the micro benchmarks:

```shell
./mvnw -Pjmh clean package

java -Dgcs.integration.test.bucket=$BUCKET_NAME \
     -Dgcs.integration.test.project-id=$PROJECT_ID \
     -Dgcs.integration.test.bucket.folder=$FOLDER_NAME \
     -jar core/target/benchmarks.jar
```

### Micro benchmark results
#### Parquet Footer Parsing

Parquet File Size   |   Footer Prefetch Disabled    | Footer Prefetch Enabled   |   Performane Gain
--- |   --- |   --- |   ---
3MB |   82.58   |   57.99   |   29.78%
30MB    |   84.24   | 56.3  |   33.17%
300MB   |   95.3    |   60.4    |   36.62

#### Parquet record read

Parquet File Size   | Vectored IO Disabled | Vectored IO + Footer Prefetch Enabled |   Performane Gain
--- |----------------------|---------------------------------------|   ---
3MB | 228.58               | 156.42                                |   31.57%
30MB    | 477.56               | 371.87                                |   22.13%
300MB   | 2865.27              | 2562.71                               |   10.56%
3000MB  | 28747.96  |   25263.00    |   12.12%

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](docs/contributing.md) for more details on how to get started.

## Security

If you discover a potential security issue in this project, please notify us by following the instructions in [SECURITY.md](SECURITY.md).

## Code of Conduct

This project has adopted the Google Open Source Community Guidelines. Please see [code-of-conduct.md](docs/code-of-conduct.md).

## License

This library is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for more details.
