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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.gcs.analyticscore.common.ConfigurationUtil;
import java.util.Map;
import java.util.Optional;

/** Configuration options for Gcs vectored read. */
@AutoValue
public abstract class GcsVectoredReadOptions {

  private static final String MAX_MERGE_GAP_KEY =
      "analytics-core.read.vectored.range.merge-gap.max-bytes";
  private static final String MAX_MERGE_SIZE_KEY =
      "analytics-core.read.vectored.range.merged-size.max-bytes";

  // The shortest distance allowed between chunks for them to be merged
  abstract int getMaxMergeGap();

  // The max allowed size of the combined chunk.
  abstract int getMaxMergeSize();

  public abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_GcsVectoredReadOptions.Builder()
        .setMaxMergeGap(4 * 1024) // 4 KB
        .setMaxMergeSize(8 * 1024 * 1024); // 8 MB
  }

  public static GcsVectoredReadOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    checkNotNull(analyticsCoreOptions, "analyticsCoreOptions cannot be null");
    checkNotNull(prefix, "prefix cannot be null");

    Builder optionsBuilder = builder();
    String maxMergeGapKey = prefix + MAX_MERGE_GAP_KEY;
    Optional<String> maxMergeGap =
        ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, maxMergeGapKey);
    if (maxMergeGap.isPresent()) {
      optionsBuilder.setMaxMergeGap(
          ConfigurationUtil.safeParseInteger(maxMergeGapKey, maxMergeGap.get()));
    }
    String maxMergeSizeKey = prefix + MAX_MERGE_SIZE_KEY;
    Optional<String> maxMergeSize =
        ConfigurationUtil.getTrimmedValue(analyticsCoreOptions, maxMergeSizeKey);
    if (maxMergeSize.isPresent()) {
      optionsBuilder.setMaxMergeSize(
          ConfigurationUtil.safeParseInteger(maxMergeSizeKey, maxMergeSize.get()));
    }

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsVectoredReadOptions}. */
  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setMaxMergeGap(int maxMergeGap);

    abstract Builder setMaxMergeSize(int maxMergeSize);

    abstract GcsVectoredReadOptions build();
  }
}
