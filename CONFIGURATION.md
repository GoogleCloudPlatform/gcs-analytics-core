# Configuration

This document outlines the key configuration properties for the GCS Analytics Core library.

## Configuration Properties

All configuration properties can be prefixed with a common string, e.g., `gcs.`. This prefix is not included in the tables below.

### General GCS Client Options

These properties govern the core connections, identity, and access parameters between your compute environment and Google Cloud Storage.

| Property | Description | Default Value |
| :--- | :--- | :--- |
| `client-lib-token` | Client library token. | - |
| `service.host` | The GCS service host. | - |
| `user-agent` | The user agent string. | - |
| `project-id` | The Google Cloud project ID for the GCS client. | - |
| `user-project` | Project ID whose Google Cloud Project's billing account should be charged for the operation being executed. | - |
| `decryption-key` | Decryption key for the object. | - |

### Authentication and Network Proxy Options

These properties configure standard Google Cloud authentication, OAuth2 token renewal, static service account impersonation, and HTTP network proxy connectivity.

> [!WARNING]
> `analytics-core.auth.client-secret`, `analytics-core.auth.refresh-token` and `analytics-core.auth.proxy.password` are secrets. Supply them through a secret store appropriate to your deployment, such as the Hadoop credential provider or a Spark secret, rather than committing them to a properties file.

| Property | Description | Default Value |
| :--- | :--- | :--- |
| `analytics-core.auth.type` | Google Cloud authentication type. Supported values: `APPLICATION_DEFAULT`, `COMPUTE_ENGINE`, `SERVICE_ACCOUNT_JSON_KEYFILE`, `WORKLOAD_IDENTITY_FEDERATION`, `USER_CREDENTIALS`, `UNAUTHENTICATED`. | `APPLICATION_DEFAULT` |
| `analytics-core.auth.service-account-json-keyfile` | Path to a Service Account JSON keyfile. Required when `analytics-core.auth.type` is `SERVICE_ACCOUNT_JSON_KEYFILE`. | - |
| `analytics-core.auth.workload-identity-federation.credential-config-file` | Path to a Workload Identity Federation credential configuration JSON file. Required when `analytics-core.auth.type` is `WORKLOAD_IDENTITY_FEDERATION`. | - |
| `analytics-core.auth.client-id` | OAuth2 client ID. Required when `analytics-core.auth.type` is `USER_CREDENTIALS`. | - |
| `analytics-core.auth.client-secret` | OAuth2 client secret. Required when `analytics-core.auth.type` is `USER_CREDENTIALS`. | - |
| `analytics-core.auth.refresh-token` | OAuth2 refresh token. Required when `analytics-core.auth.type` is `USER_CREDENTIALS`. | - |
| `analytics-core.auth.impersonation-service-account` | Service account email to impersonate via static service account impersonation. | - |
| `analytics-core.auth.token-server-uri` | Custom token server URI used to refresh OAuth2 access tokens. | - |
| `analytics-core.auth.proxy.address` | HTTP proxy address of the form `[https?://]hostname:port`. | - |
| `analytics-core.auth.proxy.username` | Username for authenticating with the HTTP proxy. | - |
| `analytics-core.auth.proxy.password` | Password for authenticating with the HTTP proxy. | - |
| `analytics-core.auth.http.connect-timeout-ms` | Connect timeout for authentication HTTP requests, in milliseconds. Must be positive. | `5000` |
| `analytics-core.auth.http.read-timeout-ms` | Socket read timeout for authentication HTTP requests, in milliseconds. Must be positive. | `5000` |

### Caching and Prefetching

These settings control how aggressively the library prefetches and caches metadata (like Parquet footers) and small objects in memory. Proper configuration here significantly reduces latency and redundant network calls during metadata discovery phases.

| Property | Description | Default Value |
| :--- | :--- | :--- |
| `analytics-core.footer.prefetch.enabled` | Controls whether footer prefetching is enabled. | `true` |
| `analytics-core.small-file.footer.prefetch.size-bytes` | Footer prefetch size (in bytes) for files up to 1 GB. | `51200` (50 KB) |
| `analytics-core.large-file.footer.prefetch.size-bytes` | Footer prefetch size (in bytes) for files larger than 1 GB. | `1048576` (1 MB) |
| `analytics-core.footer.cache.enabled` | Controls whether the Parquet footer cache is enabled. | `false` |
| `analytics-core.footer.cache.max-size-bytes`                 | The maximum capacity (in bytes) to hold in the Parquet footer cache.                        | `104857600` (100 MB) |
| `analytics-core.small-file.cache.threshold-bytes` | Threshold (in bytes) below which small files are cached entirely. | `1048576` (1 MB) |
| `analytics-core.small-file.cache.enabled` | Controls whether the small object cache is enabled. | `false` |
| `analytics-core.small-file.cache.max-size-bytes` | The maximum capacity (in bytes) to hold in the small object cache. | `209715200` (200 MB) |

### Read Performance and I/O Tuning

These parameters fine-tune the low-level data streaming behavior. They allow you to optimize thread concurrency, heuristic file access patterns, and vectored I/O merging to maximize data throughput against GCS.

| Property | Description | Default Value |
| :--- | :--- | :--- |
| `channel.read.chunk-size-bytes` | Chunk size for GCS channel reads. | - |
| `analytics-core.read.thread.count` | Number of threads for parallel read operations like vectored IO. | `16` |
| `analytics-core.read.vectored.range.merge-gap.max-bytes` | Maximum gap (in bytes) between ranges to merge in vectored reads. | `4096` (4 KB) |
| `analytics-core.read.vectored.range.merged-size.max-bytes` | Maximum size (in bytes) of a merged range in vectored reads. | `8388608` (8 MB) |
| `analytics-core.read.inplace-seek-limit-bytes` | In-place seek limit (in bytes). | `131072` (128 KB) |
| `analytics-core.read.file-access-pattern` | File access pattern. Supported values: `RANDOM`, `SEQUENTIAL`, `AUTO_SEQUENTIAL`, `AUTO_RANDOM`. | `AUTO_SEQUENTIAL` |
| `analytics-core.adaptive-read.sequential-read-threshold` | Threshold for number of sequential reads to switch to sequential mode. | `3` |
| `analytics-core.random-read.min-request-size` | Minimum request size for random reads. If the requested read size is smaller, it reads up to this size. | `131072` (128 KB) |

### Telemetry and Monitoring

These settings enable the emission of deep internal metrics—such as cache hit rates, operational durations, and throughput—to local logging consoles or distributed OpenTelemetry backends like Google Cloud Monitoring.

| Property | Description | Default Value |
| :--- | :--- | :--- |
| `analytics-core.telemetry.logging.enabled` | Controls whether logging telemetry reporter is enabled. | `false` |
| `analytics-core.telemetry.logging.level` | Specifies the log level for logging telemetry events. Supported: `TRACE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`. | `DEBUG` |
| `analytics-core.telemetry.opentelemetry.enabled` | Controls whether OpenTelemetry integration is enabled. | `false` |
| `analytics-core.telemetry.opentelemetry.provider-type` | Specifies the OpenTelemetry provider type. Supported: `GLOBAL`, `LOGGING`, `CLOUD_MONITORING`. | `GLOBAL` |
| `analytics-core.telemetry.opentelemetry.export-interval-seconds` | The export interval in seconds for OpenTelemetry periodic metric readers. | `60` |
| `analytics-core.project-id` | Google Cloud project ID for exporting OpenTelemetry metrics (specifically for `CLOUD_MONITORING`). | - |

## Notes

* **Cloud Monitoring Flush Timeout**: When using `CLOUD_MONITORING` as the OpenTelemetry provider type, closing the last active GCS client instance triggers a telemetry flush. This operation is synchronous and can block the closing thread for up to 10 seconds to ensure all remaining metrics are successfully exported to Google Cloud Monitoring before the application shuts down.
