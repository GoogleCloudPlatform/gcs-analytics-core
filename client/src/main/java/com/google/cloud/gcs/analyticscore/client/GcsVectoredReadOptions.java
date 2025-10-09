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

/** Configuration options for Gcs vectored read. */
@AutoValue
public abstract class GcsVectoredReadOptions {

  private static final String MAX_MERGE_GAP_KEY =
      "analytics-core.read.vectored.min.range.seek.size";
  private static final String MAX_MERGE_SIZE_KEY =
      "analytics-core.read.vectored.merged.range.max.size";

  // The shortest distance allowed between chunks for them to be merged
  abstract int getMaxMergeGap();

  // The max allowed size of the combined chunk.
  abstract int getMaxMergeSize();

  static Builder builder() {
    return new AutoValue_GcsVectoredReadOptions.Builder()
        .setMaxMergeGap(4 * 1024) // 4 KB
        .setMaxMergeSize(8 * 1024 * 1024); // 8 MB
  }

  public static GcsVectoredReadOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    Builder optionsBuilder = builder();
    if (analyticsCoreOptions.containsKey(prefix + MAX_MERGE_GAP_KEY)) {
      optionsBuilder.setMaxMergeGap(
          Integer.parseInt(analyticsCoreOptions.get(prefix + MAX_MERGE_GAP_KEY)));
    }
    if (analyticsCoreOptions.containsKey(prefix + MAX_MERGE_SIZE_KEY)) {
      optionsBuilder.setMaxMergeSize(
          Integer.parseInt(analyticsCoreOptions.get(prefix + MAX_MERGE_SIZE_KEY)));
    }

    return optionsBuilder.build();
  }

  /** Builder for {@link GcsVectoredReadOptions}. */
  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setMaxMergeGap(int minMergeDistance);

    abstract Builder setMaxMergeSize(int maxMergeSize);

    abstract GcsVectoredReadOptions build();
  }
}
