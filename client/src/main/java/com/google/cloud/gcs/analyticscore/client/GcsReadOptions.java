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
  private static final String FOOTER_PREFETCH_SIZE = "footer.prefetch.size";
  private static final String SMALL_OBJECT_CACHE_KEY =
      "footer.prefetch.small-object-caching.enabled";

  private static final long DEFAULT_FOOTER_PREFETCH_SIZE = 2097152; // 2mb
  private static final boolean DEFAULT_SMALL_OBJECT_CACHE = true;

  public abstract Optional<Integer> getChunkSize();

  public abstract Optional<String> getDecryptionKey();

  public abstract Optional<String> getProjectId();

  public abstract long getFooterPrefetchSize();

  public abstract boolean isSmallObjectCache();

  public abstract GcsVectoredReadOptions getGcsVectoredReadOptions();

  public static Builder builder() {
    return new AutoValue_GcsReadOptions.Builder()
        .setGcsVectoredReadOptions(GcsVectoredReadOptions.builder().build())
        .setFooterPrefetchSize(DEFAULT_FOOTER_PREFETCH_SIZE)
        .setSmallObjectCache(DEFAULT_SMALL_OBJECT_CACHE);
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
    if (analyticsCoreOptions.containsKey(prefix + FOOTER_PREFETCH_SIZE)) {
      long prefetchSize = Long.parseLong(analyticsCoreOptions.get(prefix + FOOTER_PREFETCH_SIZE));
      if (prefetchSize > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(
            String.format(
                "prefetchSize (%d) cannot be greater than Integer.MAX_VALUE (%d)",
                prefetchSize, Integer.MAX_VALUE));
      } else {
        optionsBuilder.setFooterPrefetchSize(prefetchSize);
      }
    }
    if (analyticsCoreOptions.containsKey(prefix + SMALL_OBJECT_CACHE_KEY)) {
      optionsBuilder.setSmallObjectCache(
          Boolean.parseBoolean(analyticsCoreOptions.get(prefix + SMALL_OBJECT_CACHE_KEY)));
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

    public abstract Builder setFooterPrefetchSize(long footerPrefetchSize);

    public abstract Builder setSmallObjectCache(boolean smallObjectCache);

    public abstract GcsReadOptions build();
  }
}
