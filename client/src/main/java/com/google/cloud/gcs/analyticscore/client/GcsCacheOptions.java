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

/** Configuration options for the GCS caching layer. */
@AutoValue
public abstract class GcsCacheOptions {

  private static final String FOOTER_CACHE_ENABLED_KEY = "analytics-core.footer.cache.enabled";
  private static final String FOOTER_CACHE_MAX_SIZE_BYTES_KEY =
      "analytics-core.footer.cache.max-size-bytes";

  private static final long KB = 1024L;
  private static final long MB = 1024L * KB;

  private static final boolean DEFAULT_FOOTER_CACHE_ENABLED = true;
  private static final long DEFAULT_FOOTER_CACHE_MAX_SIZE_BYTES = 100 * MB;

  /** Returns whether the Parquet footer cache is enabled. */
  public abstract boolean isFooterCacheEnabled();

  /** Returns the maximum capacity (in bytes) to hold in the Parquet footer cache. */
  public abstract long getFooterCacheMaxSizeBytes();

  /**
   * Returns a builder for {@link GcsCacheOptions} with the same property values as this instance.
   */
  public abstract Builder toBuilder();

  /** Returns a new builder for {@link GcsCacheOptions} with default values. */
  public static Builder builder() {
    return new AutoValue_GcsCacheOptions.Builder()
        .setFooterCacheEnabled(DEFAULT_FOOTER_CACHE_ENABLED)
        .setFooterCacheMaxSizeBytes(DEFAULT_FOOTER_CACHE_MAX_SIZE_BYTES);
  }

  /** Creates a {@link GcsCacheOptions} instance from a map of configuration options. */
  public static GcsCacheOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsCacheOptions.Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_CACHE_ENABLED_KEY)) {
      optionsBuilder.setFooterCacheEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + FOOTER_CACHE_ENABLED_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_CACHE_MAX_SIZE_BYTES_KEY)) {
      optionsBuilder.setFooterCacheMaxSizeBytes(
          Long.parseLong(analyticsCoreOptions.get(prefix + FOOTER_CACHE_MAX_SIZE_BYTES_KEY)));
    }
    return optionsBuilder.build();
  }

  /** Builder for {@link GcsCacheOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {
    /** Sets whether the Parquet footer cache is enabled. */
    public abstract Builder setFooterCacheEnabled(boolean footerCacheEnabled);

    /** Sets the maximum capacity (in bytes) to hold in the Parquet footer cache. */
    public abstract Builder setFooterCacheMaxSizeBytes(long footerCacheMaxSizeBytes);

    abstract GcsCacheOptions autoBuild();

    /**
     * Builds the {@link GcsCacheOptions} instance.
     *
     * @throws IllegalArgumentException if {@code footerCacheMaxSizeBytes} is non-positive when
     *     {@code footerCacheEnabled} is {@code true}.
     */
    public GcsCacheOptions build() {
      GcsCacheOptions options = autoBuild();
      if (options.isFooterCacheEnabled()) {
        checkArgument(
            options.getFooterCacheMaxSizeBytes() > 0,
            "footerCacheMaxSizeBytes must be positive when footerCacheEnabled is true");
      }
      return options;
    }
  }
}
