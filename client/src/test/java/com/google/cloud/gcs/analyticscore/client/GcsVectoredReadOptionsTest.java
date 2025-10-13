/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.client;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class GcsVectoredReadOptionsTest {

  @Test
  void createFromOptions_withValidProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "gcs.analytics-core.read.vectored.range.merge-gap.max-bytes", "8192",
            "gcs.analytics-core.read.vectored.range.merged-size.max-bytes", "16777216");

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(8192);
    assertThat(options.getMaxMergeSize()).isEqualTo(16777216);
  }

  @Test
  void createFromOptions_withDefaultProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties = ImmutableMap.of();

    GcsVectoredReadOptions options = GcsVectoredReadOptions.createFromOptions(properties, "gcs.");

    assertThat(options.getMaxMergeGap()).isEqualTo(4 * 1024);
    assertThat(options.getMaxMergeSize()).isEqualTo(8 * 1024 * 1024);
  }
}
