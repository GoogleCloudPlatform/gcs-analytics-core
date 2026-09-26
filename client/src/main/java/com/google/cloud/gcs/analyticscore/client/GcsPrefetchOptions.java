/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.Map;

/** Configuration options for predictive prefetching. */
@AutoValue
public abstract class GcsPrefetchOptions {

  static final String ENABLED_KEY = "analytics-core.prefetch.enabled";
  static final String BUFFER_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.prefetch.buffer.cache.max-size-bytes";
  static final String BUFFER_CACHE_TTL_SECONDS_KEY =
      "analytics-core.prefetch.buffer.cache.ttl-seconds";

  private static final boolean DEFAULT_ENABLED = false;
  private static final long DEFAULT_BUFFER_CACHE_MAX_SIZE_BYTES = 2L * 1024 * 1024 * 1024; // 2 GB
  private static final long DEFAULT_BUFFER_CACHE_TTL_SECONDS = 60;

  /**
   * Returns whether predictive prefetching is enabled. When enabled, the columns a query touches
   * for a given Parquet schema are learned and prefetched ahead of the engine. Defaults to {@code
   * false}.
   */
  public abstract boolean isEnabled();

  /**
   * Returns the maximum total capacity (in bytes) of the prefetch buffer cache. Defaults to {@code
   * 2147483648} (2 GB).
   */
  public abstract long getBufferCacheMaxSizeBytes();

  /**
   * Returns how long (in seconds) a prefetched block is retained in the buffer cache after it was
   * last read or written. Defaults to {@code 60} seconds.
   */
  public abstract long getBufferCacheTtlSeconds();

  /**
   * Returns a builder for {@link GcsPrefetchOptions} with the same property values as this
   * instance.
   */
  public abstract Builder toBuilder();

  /** Returns a new builder for {@link GcsPrefetchOptions} with default values. */
  public static Builder builder() {
    return new AutoValue_GcsPrefetchOptions.Builder()
        .setEnabled(DEFAULT_ENABLED)
        .setBufferCacheMaxSizeBytes(DEFAULT_BUFFER_CACHE_MAX_SIZE_BYTES)
        .setBufferCacheTtlSeconds(DEFAULT_BUFFER_CACHE_TTL_SECONDS);
  }

  /**
   * Creates a {@link GcsPrefetchOptions} instance from a map of configuration options.
   *
   * <p>Keys absent from the map keep their default value.
   *
   * @param analyticsCoreOptions the configuration options, keyed without the {@code prefix}
   * @param prefix the prefix prepended to every recognised key
   */
  public static GcsPrefetchOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsPrefetchOptions.Builder optionsBuilder = builder();
    String enabledFullKey = prefix + ENABLED_KEY;
    if (analyticsCoreOptions.containsKey(enabledFullKey)) {
      optionsBuilder.setEnabled(Boolean.parseBoolean(analyticsCoreOptions.get(enabledFullKey)));
    }
    String bufferCacheMaxSizeBytesFullKey = prefix + BUFFER_CACHE_MAX_SIZE_BYTES_KEY;
    if (analyticsCoreOptions.containsKey(bufferCacheMaxSizeBytesFullKey)) {
      optionsBuilder.setBufferCacheMaxSizeBytes(
          Long.parseLong(analyticsCoreOptions.get(bufferCacheMaxSizeBytesFullKey)));
    }
    String bufferCacheTtlSecondsFullKey = prefix + BUFFER_CACHE_TTL_SECONDS_KEY;
    if (analyticsCoreOptions.containsKey(bufferCacheTtlSecondsFullKey)) {
      optionsBuilder.setBufferCacheTtlSeconds(
          Long.parseLong(analyticsCoreOptions.get(bufferCacheTtlSecondsFullKey)));
    }

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsPrefetchOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {
    /** Sets whether predictive prefetching is enabled. Defaults to {@code false}. */
    public abstract Builder setEnabled(boolean enabled);

    /**
     * Sets the maximum total capacity (in bytes) of the prefetch buffer cache. Defaults to {@code
     * 2147483648} (2 GB).
     */
    public abstract Builder setBufferCacheMaxSizeBytes(long bufferCacheMaxSizeBytes);

    /**
     * Sets how long (in seconds) a prefetched block is retained in the buffer cache after it was
     * last read or written. Defaults to {@code 60} seconds.
     */
    public abstract Builder setBufferCacheTtlSeconds(long bufferCacheTtlSeconds);

    abstract GcsPrefetchOptions autoBuild();

    /**
     * Builds the {@link GcsPrefetchOptions} instance.
     *
     * @throws IllegalArgumentException if {@code bufferCacheMaxSizeBytes} or {@code
     *     bufferCacheTtlSeconds} is non-positive
     */
    public GcsPrefetchOptions build() {
      GcsPrefetchOptions options = autoBuild();
      checkArgument(
          options.getBufferCacheMaxSizeBytes() > 0, "bufferCacheMaxSizeBytes must be positive");
      checkArgument(
          options.getBufferCacheTtlSeconds() > 0, "bufferCacheTtlSeconds must be positive");
      return options;
    }
  }
}
