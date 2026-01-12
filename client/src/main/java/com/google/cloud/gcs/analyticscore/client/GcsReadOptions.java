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
  private static final String DECRYPTION_KEY_KEY = "decryption-key";
  private static final String FOOTER_PREFETCH_ENABLED_KEY =
      "analytics-core.footer.prefetch.enabled";
  private static final String SMALL_FILE_FOOTER_PREFETCH_SIZE_KEY =
      "analytics-core.small-file.footer.prefetch.size-bytes";
  private static final String SMALL_FILE_CACHE_THRESHOLD_KEY =
      "analytics-core.small-file.cache.threshold-bytes";
  private static final String LARGE_FILE_FOOTER_PREFETCH_SIZE_KEY =
      "analytics-core.large-file.footer.prefetch.size-bytes";
  private static final String USER_PROJECT_KEY = "user-project";

  private static final boolean DEFAULT_FOOTER_PREFETCH_ENABLED = true;
  private static final int DEFAULT_SMALL_FILE_FOOTER_PREFETCH_SIZE = 100 * 1024; // 100kb
  private static final int DEFAULT_LARGE_FILE_FOOTER_PREFETCH_SIZE = 1024 * 1024; // 1mb
  private static final int DEFAULT_SMALL_FILE_CACHE_THRESHOLD = 0; // 0 bytes = disabled

  private static final String FILE_ACCESS_PATTERN_KEY =
      "analytics-core.read.adaptive-range.file-access-pattern";
  private static final String MIN_RANGE_REQUEST_SIZE_KEY =
      "analytics-core.read.adaptive-range.min-range-request-size-bytes";
  private static final String INPLACE_SEEK_LIMIT_KEY =
      "analytics-core.read.adaptive-range.inplace-seek-limit-bytes";
  private static final String SEQUENTIAL_RANGE_READ_THRESHOLD_KEY =
      "analytics-core.read.adaptive-range.sequential-range-read-threshold";
  private static final String ADAPTIVE_RANGE_READ_ENABLED_KEY =
      "analytics-core.read.adaptive-range.enabled";

  private static final FileAccessPattern DEFAULT_FILE_ACCESS_PATTERN = FileAccessPattern.AUTO;
  private static final int DEFAULT_MIN_RANGE_REQUEST_SIZE = 2 * 1024 * 1024; // 2MB
  private static final int DEFAULT_INPLACE_SEEK_LIMIT = 8 * 1024 * 1024; // 8MB
  private static final int DEFAULT_SEQUENTIAL_RANGE_READ_THRESHOLD = 1;

  public abstract Optional<Integer> getChunkSize();

  public abstract Optional<String> getDecryptionKey();

  public abstract Optional<String> getUserProjectId();

  public abstract int getFooterPrefetchSizeSmallFile();

  public abstract int getFooterPrefetchSizeLargeFile();

  public abstract boolean isFooterPrefetchEnabled();

  public abstract int getSmallObjectCacheSize();

  public abstract GcsVectoredReadOptions getGcsVectoredReadOptions();

  public abstract FileAccessPattern getFileAccessPattern();

  public abstract int getMinRangeRequestSize();

  public abstract int getInplaceSeekLimit();

  public abstract int getSequentialRangeReadThreshold();

  public abstract Boolean getAdaptiveRangeReadEnabled();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_GcsReadOptions.Builder()
        .setGcsVectoredReadOptions(GcsVectoredReadOptions.builder().build())
        .setFooterPrefetchEnabled(DEFAULT_FOOTER_PREFETCH_ENABLED)
        .setFooterPrefetchSizeSmallFile(DEFAULT_SMALL_FILE_FOOTER_PREFETCH_SIZE)
        .setFooterPrefetchSizeLargeFile(DEFAULT_LARGE_FILE_FOOTER_PREFETCH_SIZE)
        .setSmallObjectCacheSize(DEFAULT_SMALL_FILE_CACHE_THRESHOLD)
        .setFileAccessPattern(DEFAULT_FILE_ACCESS_PATTERN)
        .setMinRangeRequestSize(DEFAULT_MIN_RANGE_REQUEST_SIZE)
        .setInplaceSeekLimit(DEFAULT_INPLACE_SEEK_LIMIT)
        .setSequentialRangeReadThreshold(DEFAULT_SEQUENTIAL_RANGE_READ_THRESHOLD)
        .setAdaptiveRangeReadEnabled(false);
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
    if (analyticsCoreOptions.containsKey(prefix + USER_PROJECT_KEY)) {
      optionsBuilder.setUserProjectId(analyticsCoreOptions.get(prefix + USER_PROJECT_KEY));
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
    if (analyticsCoreOptions.containsKey(prefix + FILE_ACCESS_PATTERN_KEY)) {
      optionsBuilder.setFileAccessPattern(
          FileAccessPattern.valueOf(
              analyticsCoreOptions.get(prefix + FILE_ACCESS_PATTERN_KEY).toUpperCase()));
    }
    if (analyticsCoreOptions.containsKey(prefix + MIN_RANGE_REQUEST_SIZE_KEY)) {
      optionsBuilder.setMinRangeRequestSize(
          safeParseInteger(analyticsCoreOptions, prefix + MIN_RANGE_REQUEST_SIZE_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + INPLACE_SEEK_LIMIT_KEY)) {
      optionsBuilder.setInplaceSeekLimit(
          safeParseInteger(analyticsCoreOptions, prefix + INPLACE_SEEK_LIMIT_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + SEQUENTIAL_RANGE_READ_THRESHOLD_KEY)) {
      optionsBuilder.setSequentialRangeReadThreshold(
          safeParseInteger(analyticsCoreOptions, prefix + SEQUENTIAL_RANGE_READ_THRESHOLD_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + ADAPTIVE_RANGE_READ_ENABLED_KEY)) {
      optionsBuilder.setAdaptiveRangeReadEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + ADAPTIVE_RANGE_READ_ENABLED_KEY)));
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

    public abstract Builder setUserProjectId(String userProjectId);

    public abstract Builder setGcsVectoredReadOptions(GcsVectoredReadOptions vectoredReadOptions);

    public abstract Builder setFooterPrefetchEnabled(boolean footerPrefetchEnabled);

    public abstract Builder setFooterPrefetchSizeSmallFile(int footerPrefetchSizeSmallFile);

    public abstract Builder setFooterPrefetchSizeLargeFile(int footerPrefetchSizeLargeFile);

    public abstract Builder setSmallObjectCacheSize(int smallObjectCacheSize);

    public abstract Builder setFileAccessPattern(FileAccessPattern fileAccessPattern);

    public abstract Builder setMinRangeRequestSize(int minRangeRequestSize);

    public abstract Builder setInplaceSeekLimit(int inplaceSeekLimit);

    public abstract Builder setSequentialRangeReadThreshold(int sequentialRangeReadThreshold);

    public abstract Builder setAdaptiveRangeReadEnabled(Boolean adaptiveRangeReadEnabled);

    public abstract GcsReadOptions build();
  }
}
