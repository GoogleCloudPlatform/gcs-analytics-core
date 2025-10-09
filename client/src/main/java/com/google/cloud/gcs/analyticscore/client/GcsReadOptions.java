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
  private static final String PROJECT_ID_KEY = "project.id";
  private static final String FOOTER_PREFETCH_ENABLED_KEY = "footer.prefetch.enabled";
  private static final String FOOTER_PREFETCH_SIZE = "footer.prefetch.size.small-file";
  private static final String FOOTER_PREFETCH_SIZE_LARGE_FILE = "footer.prefetch.size.large-file";
  private static final String SMALL_OBJECT_CACHE_KEY = "small-object-caching.enabled";
  private static final String SMALL_OBJECT_CACHE_SIZE_KEY = "small-object-caching.size";

  private static final boolean DEFAULT_FOOTER_PREFETCH_ENABLED = true;
  private static final long DEFAULT_FOOTER_PREFETCH_SIZE_SMALL_FILE = 100 * 1024; // 100kb
  private static final long DEFAULT_FOOTER_PREFETCH_SIZE_LARGE_FILE = 1024 * 1024; // 1mb
  private static final boolean DEFAULT_SMALL_OBJECT_CACHE = true;
  private static final long DEFAULT_SMALL_OBJECT_CACHE_SIZE = 1024 * 1024; // 1mb

  public abstract Optional<Integer> getChunkSize();

  public abstract Optional<String> getDecryptionKey();

  public abstract Optional<String> getProjectId();

  public abstract long getFooterPrefetchSizeSmallFile();

  public abstract long getFooterPrefetchSizeLargeFile();

  public abstract boolean isFooterPrefetchEnabled();

  public abstract boolean isSmallObjectCacheEnabled();

  public abstract long getSmallObjectCacheSize();

  public abstract GcsVectoredReadOptions getGcsVectoredReadOptions();

  public static Builder builder() {
    return new AutoValue_GcsReadOptions.Builder()
        .setGcsVectoredReadOptions(GcsVectoredReadOptions.builder().build())
        .setFooterPrefetchEnabled(DEFAULT_FOOTER_PREFETCH_ENABLED)
        .setFooterPrefetchSizeSmallFile(DEFAULT_FOOTER_PREFETCH_SIZE_SMALL_FILE)
        .setFooterPrefetchSizeLargeFile(DEFAULT_FOOTER_PREFETCH_SIZE_LARGE_FILE)
        .setSmallObjectCacheEnabled(DEFAULT_SMALL_OBJECT_CACHE)
        .setSmallObjectCacheSize(DEFAULT_SMALL_OBJECT_CACHE_SIZE);
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
    if (analyticsCoreOptions.containsKey(prefix + PROJECT_ID_KEY)) {
      optionsBuilder.setProjectId(analyticsCoreOptions.get(prefix + PROJECT_ID_KEY));
    }
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_PREFETCH_ENABLED_KEY)) {
      optionsBuilder.setFooterPrefetchEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + FOOTER_PREFETCH_ENABLED_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_PREFETCH_SIZE)) {
      long prefetchSizeSmallFile =
          Long.parseLong(analyticsCoreOptions.get(prefix + FOOTER_PREFETCH_SIZE));
      if (prefetchSizeSmallFile > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(
            String.format(
                "prefetchSizeForSmallFile (%d) cannot be greater than Integer.MAX_VALUE (%d)",
                prefetchSizeSmallFile, Integer.MAX_VALUE));
      } else {
        optionsBuilder.setFooterPrefetchSizeSmallFile(prefetchSizeSmallFile);
      }
    }
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_PREFETCH_SIZE_LARGE_FILE)) {
      long prefetchSizeLargeFile =
          Long.parseLong(analyticsCoreOptions.get(prefix + FOOTER_PREFETCH_SIZE_LARGE_FILE));
      if (prefetchSizeLargeFile > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(
            String.format(
                "prefetchSizeForLargeFile (%d) cannot be greater than Integer.MAX_VALUE (%d)",
                prefetchSizeLargeFile, Integer.MAX_VALUE));
      } else {
        optionsBuilder.setFooterPrefetchSizeLargeFile(prefetchSizeLargeFile);
      }
    }
    if (analyticsCoreOptions.containsKey(prefix + SMALL_OBJECT_CACHE_KEY)) {
      optionsBuilder.setSmallObjectCacheEnabled(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + SMALL_OBJECT_CACHE_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + SMALL_OBJECT_CACHE_SIZE_KEY)) {
        optionsBuilder.setSmallObjectCacheSize(
                Long.parseLong(analyticsCoreOptions.get(prefix + SMALL_OBJECT_CACHE_SIZE_KEY)));
    }
    optionsBuilder.setGcsVectoredReadOptions(
        GcsVectoredReadOptions.createFromOptions(analyticsCoreOptions, prefix));

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsReadOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setChunkSize(Integer chunkSize);

    public abstract Builder setDecryptionKey(String decryptionKey);

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setGcsVectoredReadOptions(GcsVectoredReadOptions vectoredReadOptions);

    public abstract Builder setFooterPrefetchEnabled(boolean footerPrefetchEnabled);

    public abstract Builder setFooterPrefetchSizeSmallFile(long footerPrefetchSizeSmallFile);

    public abstract Builder setFooterPrefetchSizeLargeFile(long footerPrefetchSizeLargeFile);

    public abstract Builder setSmallObjectCacheEnabled(boolean smallObjectCacheEnabled);

    public abstract Builder setSmallObjectCacheSize(long smallObjectCacheSize);

    public abstract GcsReadOptions build();
  }
}
