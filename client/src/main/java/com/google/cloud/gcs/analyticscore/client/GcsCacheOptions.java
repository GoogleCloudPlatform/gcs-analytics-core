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
  private static final String FOOTER_CACHE_MAX_ENTRIES_KEY =
      "analytics-core.footer.cache.max-entries";

  private static final String BUCKET_CAPABILITIES_CACHE_MAX_SIZE_KEY =
      "analytics-core.bucket-capabilities.cache.max-size";
  private static final String BUCKET_CAPABILITIES_CACHE_MAX_ENTRY_AGE_MINUTES_KEY =
      "analytics-core.bucket-capabilities.cache.max-entry-age-minutes";

  private static final boolean DEFAULT_FOOTER_CACHE_ENABLED = true;
  private static final int DEFAULT_FOOTER_CACHE_MAX_ENTRIES = 100;

  private static final int DEFAULT_BUCKET_CAPABILITIES_CACHE_MAX_SIZE = 1000;
  private static final long DEFAULT_BUCKET_CAPABILITIES_CACHE_MAX_ENTRY_AGE_MINUTES = 5;

  /** Returns whether the Parquet footer cache is enabled. */
  public abstract boolean isFooterCacheEnabled();

  /** Returns the maximum number of entries to hold in the Parquet footer cache. */
  public abstract int getFooterCacheMaxEntries();

  /** Returns the maximum number of entries to hold in the bucket capabilities cache. */
  public abstract int getBucketCapabilitiesCacheMaxSize();

  /** Returns the maximum age (in minutes) of an entry in the bucket capabilities cache. */
  public abstract long getBucketCapabilitiesCacheMaxEntryAgeMinutes();

  /**
   * Returns a builder for {@link GcsCacheOptions} with the same property values as this instance.
   */
  public abstract Builder toBuilder();

  /** Returns a new builder for {@link GcsCacheOptions} with default values. */
  public static Builder builder() {
    return new AutoValue_GcsCacheOptions.Builder()
        .setFooterCacheEnabled(DEFAULT_FOOTER_CACHE_ENABLED)
        .setFooterCacheMaxEntries(DEFAULT_FOOTER_CACHE_MAX_ENTRIES)
        .setBucketCapabilitiesCacheMaxSize(DEFAULT_BUCKET_CAPABILITIES_CACHE_MAX_SIZE)
        .setBucketCapabilitiesCacheMaxEntryAgeMinutes(
            DEFAULT_BUCKET_CAPABILITIES_CACHE_MAX_ENTRY_AGE_MINUTES);
  }

  /** Creates a {@link GcsCacheOptions} instance from a map of configuration options. */
  public static GcsCacheOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsCacheOptions.Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_CACHE_ENABLED_KEY)) {
      optionsBuilder.setFooterCacheEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + FOOTER_CACHE_ENABLED_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_CACHE_MAX_ENTRIES_KEY)) {
      optionsBuilder.setFooterCacheMaxEntries(
          Integer.parseInt(analyticsCoreOptions.get(prefix + FOOTER_CACHE_MAX_ENTRIES_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + BUCKET_CAPABILITIES_CACHE_MAX_SIZE_KEY)) {
      optionsBuilder.setBucketCapabilitiesCacheMaxSize(
          Integer.parseInt(
              analyticsCoreOptions.get(prefix + BUCKET_CAPABILITIES_CACHE_MAX_SIZE_KEY)));
    }
    if (analyticsCoreOptions.containsKey(
        prefix + BUCKET_CAPABILITIES_CACHE_MAX_ENTRY_AGE_MINUTES_KEY)) {
      optionsBuilder.setBucketCapabilitiesCacheMaxEntryAgeMinutes(
          Long.parseLong(
              analyticsCoreOptions.get(
                  prefix + BUCKET_CAPABILITIES_CACHE_MAX_ENTRY_AGE_MINUTES_KEY)));
    }
    return optionsBuilder.build();
  }

  /** Builder for {@link GcsCacheOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {
    /** Sets whether the Parquet footer cache is enabled. */
    public abstract Builder setFooterCacheEnabled(boolean footerCacheEnabled);

    /** Sets the maximum number of entries to hold in the Parquet footer cache. */
    public abstract Builder setFooterCacheMaxEntries(int footerCacheMaxEntries);

    /** Sets the maximum number of entries to hold in the bucket capabilities cache. */
    public abstract Builder setBucketCapabilitiesCacheMaxSize(int bucketCapabilitiesCacheMaxSize);

    /** Sets the maximum age (in minutes) of an entry in the bucket capabilities cache. */
    public abstract Builder setBucketCapabilitiesCacheMaxEntryAgeMinutes(
        long bucketCapabilitiesCacheMaxEntryAgeMinutes);

    abstract GcsCacheOptions autoBuild();

    /**
     * Builds the {@link GcsCacheOptions} instance.
     *
     * @throws IllegalArgumentException if {@code footerCacheMaxEntries} is non-positive when {@code
     *     footerCacheEnabled} is {@code true}.
     */
    public GcsCacheOptions build() {
      GcsCacheOptions options = autoBuild();
      if (options.isFooterCacheEnabled()) {
        checkArgument(
            options.getFooterCacheMaxEntries() > 0,
            "footerCacheMaxEntries must be positive when footerCacheEnabled is true");
      }
      checkArgument(
          options.getBucketCapabilitiesCacheMaxSize() > 0,
          "bucketCapabilitiesCacheMaxSize must be positive");
      checkArgument(
          options.getBucketCapabilitiesCacheMaxEntryAgeMinutes() >= 0,
          "bucketCapabilitiesCacheMaxEntryAgeMinutes must be non-negative");
      return options;
    }
  }
}
