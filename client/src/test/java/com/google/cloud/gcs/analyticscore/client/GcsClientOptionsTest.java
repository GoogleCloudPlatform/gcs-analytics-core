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
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class GcsClientOptionsTest {

  @Test
  void builder_shouldCreateDefaultOptions() {
    GcsClientOptions options = GcsClientOptions.builder().build();

    assertThat(options.getProjectId()).isEqualTo(Optional.empty());
    assertThat(options.getClientLibToken()).isEqualTo(Optional.empty());
    assertThat(options.getServiceHost()).isEqualTo(Optional.empty());
    assertThat(options.getUserAgent()).isEqualTo(Optional.empty());
    assertThat(options.getAdaptiveRangeReadEnabled()).isFalse();
    assertThat(options.getGcsReadOptions()).isNotNull();
  }

  @Test
  void createFromOptions_withAllProperties_shouldCreateCorrectOptions() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("gcs.project-id", "test-project")
            .put("gcs.client-lib-token", "test-token")
            .put("gcs.service.host", "test-host")
            .put("gcs.user-agent", "test-agent")
            .put("gcs.analytics-core.adaptive-range-read-enabled", "true")
            .put("gcs.channel.read.chunk-size-bytes", "1024")
            .build();
    String prefix = "gcs.";

    GcsClientOptions options = GcsClientOptions.createFromOptions(properties, prefix);

    assertThat(options.getProjectId()).isEqualTo(Optional.of("test-project"));
    assertThat(options.getClientLibToken()).isEqualTo(Optional.of("test-token"));
    assertThat(options.getServiceHost()).isEqualTo(Optional.of("test-host"));
    assertThat(options.getUserAgent()).isEqualTo(Optional.of("test-agent"));
    assertThat(options.getAdaptiveRangeReadEnabled()).isTrue();
    assertThat(options.getGcsReadOptions().getChunkSize()).isEqualTo(Optional.of(1024));
  }

  @Test
  void createFromOptions_withNoProperties_shouldCreateDefaultOptions() {
    Map<String, String> properties = ImmutableMap.of();
    String prefix = "gcs.";

    GcsClientOptions options = GcsClientOptions.createFromOptions(properties, prefix);

    assertThat(options.getProjectId()).isEqualTo(Optional.empty());
    assertThat(options.getClientLibToken()).isEqualTo(Optional.empty());
    assertThat(options.getServiceHost()).isEqualTo(Optional.empty());
    assertThat(options.getUserAgent()).isEqualTo(Optional.empty());
    assertThat(options.getAdaptiveRangeReadEnabled()).isFalse();
    assertThat(options.getGcsReadOptions()).isNotNull();
  }

  @Test
  void createFromOptions_withPartialProperties_shouldCreateCorrectOptions() {
    Map<String, String> properties =
        ImmutableMap.of(
            "gcs.project-id", "test-project",
            "gcs.analytics-core.adaptive-range-read-enabled", "true");
    String prefix = "gcs.";

    GcsClientOptions options = GcsClientOptions.createFromOptions(properties, prefix);

    assertThat(options.getProjectId()).isEqualTo(Optional.of("test-project"));
    assertThat(options.getClientLibToken()).isEqualTo(Optional.empty());
    assertThat(options.getAdaptiveRangeReadEnabled()).isTrue();
  }
}
