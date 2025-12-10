# Configuration

This document outlines the key configuration properties for the GCS Analytics Core library.

## Configuration Properties

All configuration properties can be prefixed with a common string, e.g., `gcs.`. This prefix is not included in the table below.

| Property                                                   | Description                                                                                                 | Default Value |
|:-----------------------------------------------------------|:------------------------------------------------------------------------------------------------------------| :------------ |
| `client.type`                                              | The type of GCS client to use. Options are `HTTP_CLIENT` or `GRPC_CLIENT`.                                | `HTTP_CLIENT` |
| `client.grpc.direct-path-enabled`                          | Controls whether to attempt to use DirectPath for GRPC requests. Only applicable if `client.type` is `GRPC_CLIENT`. | `true`        |
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
