# GCS Analytics Core - Developer Guide

Welcome to the `gcs-analytics-core` developer guide. This document provides the necessary information for integrating and configuring the library within your query engines (like Spark) or data lake file system abstractions (such as Hadoop's `GoogleHadoopFileSystem` or Apache Iceberg's `GcsFileIO`).

## 1. Overview & Architecture
`gcs-analytics-core` is a library designed to optimize the performance of data analytics queries (particularly those reading columnar formats like Parquet and ORC) directly from Google Cloud Storage (GCS).

**Integration Hierarchy:**
Query engines typically do not integrate with this library directly. Instead, the architecture follows this hierarchy:
1.  **Query Engines** (e.g., Spark, Trino) interface with a standard file system abstraction.
2.  **File System Abstractions** (e.g., Hadoop's `GoogleHadoopFileSystem` or Apache Iceberg's `GcsFileIO`) manage the metadata and table formats.
3.  **`gcs-analytics-core`** is embedded within these file system abstractions to handle the low-level, optimized I/O operations against GCS.

**Important Scope Note:** This library currently focuses **exclusively on the read path**, while the **write path is currently under development**. It is designed to be embedded within larger FileSystem abstractions (such as Apache Iceberg's `GcsFileIO`). Integrators should use this library for optimized `open()` operations and delegate all writes, deletions, and namespace listings to the standard `google-cloud-storage` Java SDK.

## 2. Installation

To use `gcs-analytics-core` in your project, add the following dependency to your build system.

**Maven:**
```xml
<dependency>
    <groupId>com.google.cloud.gcs.analytics</groupId>
    <artifactId>gcs-analytics-core</artifactId>
    <version>1.4.1</version> <!-- Find the latest version at https://mvnrepository.com/artifact/com.google.cloud.gcs.analytics/gcs-analytics-core -->
</dependency>
```

**Gradle:**
```gradle
implementation 'com.google.cloud.gcs.analytics:gcs-analytics-core:1.4.1'
```

**Module Breakdown:**
*   **Client (`client/`)**: Provides foundational GCS operations and the Vectored I/O API for highly parallel byte-range reads.
*   **Core (`core/`)**: Builds upon the client to provide `GoogleCloudStorageInputStream`, a seekable stream optimized with heuristics like footer prefetching and adaptive read strategies.

## 3. Key Public Components

*   **[`GcsFileSystem`](./client/src/main/java/com/google/cloud/gcs/analyticscore/client/GcsFileSystem.java)**: The primary entry point. Manages the connection to GCS and holds all configuration and thread pools.
*   **[`GcsAnalyticsCoreOptions`](./core/src/main/java/com/google/cloud/gcs/analyticscore/core/GcsAnalyticsCoreOptions.java)**: The primary configuration object used to parse and manage library properties.
*   **[`GoogleCloudStorageInputStream`](./core/src/main/java/com/google/cloud/gcs/analyticscore/core/GoogleCloudStorageInputStream.java)**: The analytics-optimized input stream returned when opening a file.
*   *(Future)* **Write Path Components**: Future releases will introduce dedicated public components (e.g., optimized output streams) to accelerate data writes to GCS.

## 4. Configuration Guide

Configuration is primarily maintained via [`GcsAnalyticsCoreOptions`](./core/src/main/java/com/google/cloud/gcs/analyticscore/core/GcsAnalyticsCoreOptions.java) which delegates file system settings to [`GcsFileSystemOptions`](./client/src/main/java/com/google/cloud/gcs/analyticscore/client/GcsFileSystemOptions.java). See [CONFIGURATION.md](./CONFIGURATION.md) for a full list of supported properties.

Key areas of configuration include:

*   **Concurrency & Client Connections**: Manage the size of the Vectored I/O thread pool (`analytics-core.read.thread.count`).
*   **Adaptive Read Tuning**: Control how the stream predicts read behavior using `analytics-core.read.file-access-pattern` (e.g., `AUTO_SEQUENTIAL` or `AUTO_RANDOM`) and tune the in-place seek threshold (`analytics-core.read.inplace-seek-limit-bytes`) to avoid excessive connection drops.
*   **Vectored I/O Merging**: Define the heuristics for merging adjacent byte-range requests, such as the maximum allowed gap between ranges (`analytics-core.read.vectored.range.merge-gap.max-bytes`).
*   **Caching ([`GcsCacheOptions`](./client/src/main/java/com/google/cloud/gcs/analyticscore/client/GcsCacheOptions.java))**: Enable and size the in-memory caches for small objects (`analytics-core.small-file.cache.enabled`) and Parquet footers to eliminate redundant network calls across tasks.
*   **Telemetry ([`TelemetryOptions`](./common/src/main/java/com/google/cloud/gcs/analyticscore/common/telemetry/TelemetryOptions.java))**: Wire up observability by enabling OpenTelemetry (`analytics-core.telemetry.opentelemetry.enabled`) to export deep internal metrics (like cache hit rates and stream durations) to backends like Google Cloud Monitoring or standard loggers.

## 5. Core Optimizations

`gcs-analytics-core` provides several optimizations to reduce network round-trips and improve throughput.

*   **Footer Prefetching**: Heuristically fetches file footers during the initial open request. [Read the deep-dive ➔](./docs/optimizations/footer-prefetching.md)
*   **Vectored I/O**: Dispatches parallel reads and merges adjacent requests into single HTTP calls. [Read the deep-dive ➔](./docs/optimizations/vectored-io.md)
*   **Adaptive Read Strategy**: Dynamically switches between sequential and random access patterns based on read behavior. [Read the deep-dive ➔](./docs/optimizations/adaptive-read-strategy.md)
*   **Caching Layer**: Configurable in-memory caching for small objects and file footers. [Read the deep-dive ➔](./docs/optimizations/caching.md)

## 6. Integration Examples

The following examples demonstrate how to initialize the file system and utilize its optimized read capabilities.

### 1. Initialization and Configuration

Configuration is typically passed in as a map of properties (often originating from Hadoop Configuration or Iceberg Catalog properties). Authentication can be provided implicitly (using Application Default Credentials) or explicitly.

```java
import com.google.cloud.gcs.analyticscore.core.GcsAnalyticsCoreOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.FileInputStream;
import java.util.Map;

// 1. Define your properties (usually loaded from a config file)
Map<String, String> properties = Map.of(
    "gcs.analytics-core.read.thread.count", "32",
    "gcs.analytics-core.footer.prefetch.enabled", "true"
);

// 2. Parse options. The prefix ("gcs.") helps filter relevant keys.
GcsAnalyticsCoreOptions options = new GcsAnalyticsCoreOptions("gcs.", properties);

// 3. Initialize the File System.
// Option A: Use Google Application Default Credentials (ADC) implicitly
GcsFileSystem gcsFileSystem = new GcsFileSystemImpl(options.getGcsFileSystemOptions());

// Option B: Explicitly pass Credentials (e.g., from a Service Account JSON)
// Credentials credentials = GoogleCredentials.fromStream(new FileInputStream("/path/to/key.json"));
// GcsFileSystem customAuthFileSystem = new GcsFileSystemImpl(credentials, options.getGcsFileSystemOptions());

// Important: Ensure the file system is closed when the application shuts down to release thread pools!
```

### 2. Telemetry Configuration

`gcs-analytics-core` supports emitting internal metrics (like cache hit rates and I/O throughput) to OpenTelemetry, simple logging, or custom implementations.

**Option A: Property-Based Configuration (OpenTelemetry / Logging)**
You can easily enable built-in reporters by passing properties to the configuration map.

```java
Map<String, String> properties = Map.of(
    // Enable simple logging of metrics (useful for debugging)
    "gcs.analytics-core.telemetry.logging.enabled", "true",
    "gcs.analytics-core.telemetry.logging.level", "INFO",

    // Or, export to Google Cloud Monitoring via OpenTelemetry
    "gcs.analytics-core.telemetry.opentelemetry.enabled", "true",
    "gcs.analytics-core.telemetry.opentelemetry.provider-type", "CLOUD_MONITORING",
    "gcs.analytics-core.telemetry.opentelemetry.export-interval-seconds", "60",
    "gcs.project-id", "my-gcp-project" // Required for CLOUD_MONITORING
);
// Passing these to GcsAnalyticsCoreOptions automatically wires up the reporters.
```
*(Other supported `provider-type` values include `GLOBAL` to use the `GlobalOpenTelemetry` instance, and `LOGGING` for local stdout periodic readers).*

**Option B: Programmatic Injection (Custom Metrics / Pre-configured OTel)**
If your engine uses a different metric system (e.g., Dropwizard, Prometheus) or you have a pre-configured OpenTelemetry instance, you can bypass the property map and use the Builder API directly.

```java
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.CustomTelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.TelemetryOptions;
import com.google.cloud.gcs.analyticscore.common.telemetry.OperationListener;
import java.util.List;

// 1. Create a custom listener that delegates to your engine's internal metrics
OperationListener myCustomMetricsAdapter = new MyEngineMetricsAdapter();

// 2. Build the TelemetryOptions programmatically
TelemetryOptions telemetryOptions = TelemetryOptions.builder()
    .setCustomTelemetryOptions(
        CustomTelemetryOptions.builder()
            .setOperationListeners(List.of(myCustomMetricsAdapter))
            .build()
    )
    .build();

// 3. Inject it directly into the File System Options builder
GcsFileSystemOptions fsOptions = GcsFileSystemOptions.builder()
    .setAnalyticsCoreTelemetryOptions(telemetryOptions)
    .build();

GcsFileSystem gcsFileSystem = new GcsFileSystemImpl(fsOptions);
```

### 3. Reading Data (Standard and Vectored)

Once the `GcsFileSystem` is initialized, you can use it to open files via the `GoogleCloudStorageInputStream`.

**Important Note on Optimizations:** To fully utilize features like Footer Prefetching, the stream must know the file's size at initialization. Therefore, it is highly recommended to use the `.create()` methods that accept a `URI` (which automatically fetches metadata) or explicitly pass a `GcsFileInfo` object. If you only pass a `GcsItemId`, certain prefetching optimizations will be silently bypassed to avoid blocking on an extra metadata network call. *(Note: In the future, this limitation will be removed by employing heuristics and reading metadata in the first read).*

```java
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;

URI fileUri = URI.create("gs://my-bucket/data/table/part-0000.parquet");

// Open the file. Passing URI automatically fetches GcsFileInfo (file size), enabling footer prefetching!
try (GoogleCloudStorageInputStream stream = GoogleCloudStorageInputStream.create(gcsFileSystem, fileUri)) {

    // Example A: Standard Seek and Read
    stream.seek(1024);
    int firstByte = stream.read();

    // Example B: Vectored I/O (Parallel Range Reads)
    GcsObjectRange range1 = GcsObjectRange.builder()
        .setOffset(1000)
        .setLength(500)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();

    GcsObjectRange range2 = GcsObjectRange.builder()
        .setOffset(8000)
        .setLength(200)
        .setByteBufferFuture(new CompletableFuture<>())
        .build();

    List<GcsObjectRange> ranges = List.of(range1, range2);

    // The stream will merge these if close enough, and fetch them in parallel using the thread pool.
    // We pass a buffer allocator so the library can allocate memory dynamically for the merged results.
    stream.readVectored(ranges, ByteBuffer::allocate);

    // Wait for the asynchronous fetch to complete
    ByteBuffer data1 = range1.getByteBufferFuture().get();
    ByteBuffer data2 = range2.getByteBufferFuture().get();
}

// Alternative (Best for performance): If your engine (like Iceberg) already knows the file size,
// pass GcsFileInfo directly to avoid the redundant metadata network call entirely:
// GoogleCloudStorageInputStream stream = GoogleCloudStorageInputStream.create(gcsFileSystem, myGcsFileInfo);
```

## 7. Performance Tuning & Best Practices

To extract the maximum performance from `gcs-analytics-core`, consider the following tuning guidelines based on your specific deployment environment:

### Thread Pool Sizing
Because reading from Google Cloud Storage is an I/O-bound operation, the thread pool should generally be sized much larger than the number of available CPU cores.
*   **The Default Formula:** By default, the library provisions `Math.max(16, 4 * availableProcessors)` threads. This provides excellent parallel network throughput while remaining safe for smaller VMs.
*   **Scaling Up:** For larger analytics nodes (e.g., 16+ vCPUs) with highly provisioned network bandwidth, you can safely scale `analytics-core.read.thread.count` up to `64` or `128`. The threads spend the majority of their time parked waiting for I/O, so context switching overhead is negligible.
*   **Predictability:** The thread pool deliberately uses a fixed size (Core Size == Max Size) with a bounded queue. This ensures that a large burst of vectored reads doesn't cause an `OutOfMemoryError` or force the JVM to spend excessive time queuing tasks before spinning up threads.

### Cache Tuning
The in-memory caches (for footers and small objects) are effective but must be tuned to respect your JVM's heap constraints.
*   **Reusability:** The caches are bound to the lifecycle of the `GcsFileSystem`. They are only effective if your engine initializes a single `GcsFileSystem` instance and shares it across multiple tasks or queries on the same node. If your engine creates a new file system instance per task, disable the caches to save memory.
*   **Memory Limits:** Always set a strict byte boundary (e.g., `analytics-core.small-file.cache.max-size-bytes`). A good rule of thumb is allocating ~100MB to 500MB per cache type on a standard executor, ensuring it doesn't compete with the engine's own memory execution pools. As a benchmark, dedicating a 500 MB small object cache on a 30-node cluster provides up to a 5% scan time improvement for the TPCDS 10TB benchmark in Apache Iceberg.

### Lifecycle Management (Crucial)
The `GcsFileSystem` instance acts as a container for long-lived resources, including the Caffeine caches and the Vectored I/O `ThreadPoolExecutor`.
*   **Always Close:** You **must** ensure that `gcsFileSystem.close()` is called when the application or executor node shuts down. Failing to do so will result in severe thread leaks, as the executor's threads are kept alive to serve background requests. Use `try-with-resources` or register a shutdown hook.
