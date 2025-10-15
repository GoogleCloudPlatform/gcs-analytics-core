/*
 * Copyright 2025 Google LLC
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

import com.google.auto.value.AutoValue;
import java.util.Map;
import java.util.Optional;

/** Configuration options for the GCS read options. */
@AutoValue
public abstract class GcsReadOptions {
  private static final String GCS_CHANNEL_READ_CHUNK_SIZE_KEY = "channel.read.chunk-size-bytes";
  private static final String DECRYPTION_KEY_KEY = "decryption.key";
  private static final String FOOTER_PREFETCH_ENABLED_KEY =
      "analytics-core.footer.prefetch.enabled";
  private static final String SMALL_FILE_FOOTER_PREFETCH_SIZE_KEY =
      "analytics-core.small-file.footer.prefetch.size-bytes";
  private static final String SMALL_FILE_CACHE_THRESHOLD_KEY =
      "analytics-core.small-file.cache.threshold-bytes";
  private static final String LARGE_FILE_FOOTER_PREFETCH_SIZE_KEY =
      "analytics-core.large-file.footer.prefetch.size-bytes";

  private static final boolean DEFAULT_FOOTER_PREFETCH_ENABLED = true;
  private static final int DEFAULT_SMALL_FILE_FOOTER_PREFETCH_SIZE = 100 * 1024; // 100kb
  private static final int DEFAULT_LARGE_FILE_FOOTER_PREFETCH_SIZE = 1024 * 1024; // 1mb
  private static final int DEFAULT_SMALL_FILE_CACHE_SIZE = 1024 * 1024; // 1mb

  public abstract Optional<Integer> getChunkSize();

  public abstract Optional<String> getDecryptionKey();

  public abstract Optional<String> getProjectId();

  public abstract int getFooterPrefetchSizeSmallFile();

  public abstract int getFooterPrefetchSizeLargeFile();

  public abstract boolean isFooterPrefetchEnabled();

  public abstract int getSmallObjectCacheSize();

  public abstract GcsVectoredReadOptions getGcsVectoredReadOptions();

  public static Builder builder() {
    return new AutoValue_GcsReadOptions.Builder()
        .setGcsVectoredReadOptions(GcsVectoredReadOptions.builder().build())
        .setFooterPrefetchEnabled(DEFAULT_FOOTER_PREFETCH_ENABLED)
        .setFooterPrefetchSizeSmallFile(DEFAULT_SMALL_FILE_FOOTER_PREFETCH_SIZE)
        .setFooterPrefetchSizeLargeFile(DEFAULT_LARGE_FILE_FOOTER_PREFETCH_SIZE)
        .setSmallObjectCacheSize(DEFAULT_SMALL_FILE_CACHE_SIZE);
  }

  public static GcsReadOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    GcsReadOptions.Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(prefix + GCS_CHANNEL_READ_CHUNK_SIZE_KEY)) {
      optionsBuilder.setChunkSize(
          Integer.parseInt(analyticsCoreOptions.get(prefix + GCS_CHANNEL_READ_CHUNK_SIZE_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + DECRYPTION_KEY_KEY)) {
      optionsBuilder.setDecryptionKey(analyticsCoreOptions.get(prefix + DECRYPTION_KEY_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + GcsClientOptions.PROJECT_ID_KEY)) {
      optionsBuilder.setProjectId(
          analyticsCoreOptions.get(prefix + GcsClientOptions.PROJECT_ID_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_PREFETCH_ENABLED_KEY)) {
      optionsBuilder.setFooterPrefetchEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + FOOTER_PREFETCH_ENABLED_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + SMALL_FILE_FOOTER_PREFETCH_SIZE_KEY)) {
      optionsBuilder.setFooterPrefetchSizeSmallFile(
          safeParseInteger(analyticsCoreOptions, prefix + SMALL_FILE_FOOTER_PREFETCH_SIZE_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + LARGE_FILE_FOOTER_PREFETCH_SIZE_KEY)) {
      optionsBuilder.setFooterPrefetchSizeLargeFile(
          safeParseInteger(analyticsCoreOptions, prefix + LARGE_FILE_FOOTER_PREFETCH_SIZE_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + SMALL_FILE_CACHE_THRESHOLD_KEY)) {
      optionsBuilder.setSmallObjectCacheSize(
          safeParseInteger(analyticsCoreOptions, prefix + SMALL_FILE_CACHE_THRESHOLD_KEY));
    }
    optionsBuilder.setGcsVectoredReadOptions(
        GcsVectoredReadOptions.createFromOptions(analyticsCoreOptions, prefix));

    return optionsBuilder.build();
  }

  private static int safeParseInteger(Map<String, String> analyticsCoreOptions, String key) {
    long value = Long.parseLong(analyticsCoreOptions.get(key));
    if (value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              "%s=%d cannot be greater than Integer.MAX_VALUE (%d)",
              key, value, Integer.MAX_VALUE));
    }
    return (int) value;
  }

  /** Builder for {@link GcsReadOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setChunkSize(Integer chunkSize);

    public abstract Builder setDecryptionKey(String decryptionKey);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setGcsVectoredReadOptions(GcsVectoredReadOptions vectoredReadOptions);

    public abstract Builder setFooterPrefetchEnabled(boolean footerPrefetchEnabled);

    public abstract Builder setFooterPrefetchSizeSmallFile(int footerPrefetchSizeSmallFile);

    public abstract Builder setFooterPrefetchSizeLargeFile(int footerPrefetchSizeLargeFile);

    public abstract Builder setSmallObjectCacheSize(int smallObjectCacheSize);

    public abstract GcsReadOptions build();
  }
}
