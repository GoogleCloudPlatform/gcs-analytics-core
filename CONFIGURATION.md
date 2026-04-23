# Configuration

This document outlines the key configuration properties for the GCS Analytics Core library.

## Configuration Properties

All configuration properties can be prefixed with a common string, e.g., `gcs.`. This prefix is not included in the table below.

| Property                                                   | Description                                                                                                 | Default Value |
|:-----------------------------------------------------------|:------------------------------------------------------------------------------------------------------------| :------------ |
| `client-lib-token`                                         | Client library token.                                                                                       | -             |
| `service.host`                                             | The GCS service host.                                                                                       | -             |
| `user-agent`                                               | The user agent string.                                                                                      | -             |
| `channel.read.chunk-size-bytes`                            | Chunk size for GCS channel reads.                                                                           | -             |
| `decryption-key`                                           | Decryption key for the object.                                                                              | -             |
| `project-id`                                               | The Google Cloud project ID for the GCS client.                                                             | -             |
| `user-project`                                             | Project ID whose Google Cloud Project's billing account should be charged for the operation being executed. | -             |
| `analytics-core.footer.prefetch.enabled`                   | Controls whether footer prefetching is enabled.                                                             | `true`        |
| `analytics-core.small-file.footer.prefetch.size-bytes`     | Footer prefetch size (in bytes) for files up to 1 GB.                                                       | 102400 (100 KB) |
| `analytics-core.large-file.footer.prefetch.size-bytes`     | Footer prefetch size (in bytes) for files larger than 1 GB.                                                 | 1048576 (1 MB)  |
| `analytics-core.small-file.cache.threshold-bytes`          | Threshold (in bytes) below which small files are cached entirely.                                           | 1048576 (1 MB)  |
| `analytics-core.read.thread.count`                         | Number of threads for parallel read operations like vectored IO.                                            | 16            |
| `analytics-core.read.vectored.range.merge-gap.max-bytes`   | Maximum gap (in bytes) between ranges to merge in vectored reads.                                           | 4096 (4 KB)   |
| `analytics-core.read.vectored.range.merged-size.max-bytes` | Maximum size (in bytes) of a merged range in vectored reads.                                                | 8388608 (8 MB)  |
| `analytics-core.telemetry.logging.enabled`                   | Enable or disable logging telemetry.                                                                        | `false`        |
| `analytics-core.telemetry.logging.level`                     | The log level to use for telemetry logs. Options: `TRACE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`.             | `DEBUG`        |
| `analytics-core.telemetry.opentelemetry.enabled`             | Enable or disable OpenTelemetry integration.                                                                | `false`        |
| `analytics-core.telemetry.opentelemetry.provider-type`       | The type of OpenTelemetry provider to use. Options: `GLOBAL`, `PRE_CONFIGURED`, `LOGGING`.                  | `GLOBAL`       |
| `analytics-core.telemetry.opentelemetry.export-interval-seconds` | The interval in seconds at which metrics are exported.                                                      | `60`          |
