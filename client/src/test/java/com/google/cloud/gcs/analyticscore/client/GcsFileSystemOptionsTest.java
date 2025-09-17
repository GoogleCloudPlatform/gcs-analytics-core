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

class GcsFileSystemOptionsTest {

  @Test
  void createFromOptions_withValidProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "fs.gs.project-id", "test-project",
            "fs.gs.client.type", "GRPC_CLIENT",
            "fs.gs.read-thread-count", "32");

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getGcsClientOptions().getProjectId().get()).isEqualTo("test-project");
    assertThat(options.getClientType()).isEqualTo(GcsFileSystemOptions.ClientType.GRPC_CLIENT);
    assertThat(options.getReadThreadCount()).isEqualTo(32);
  }

  @Test
  void createFromOptions_withDefaultProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties = ImmutableMap.of();

    GcsFileSystemOptions options = GcsFileSystemOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getGcsClientOptions().getProjectId().isEmpty()).isTrue();
    assertThat(options.getClientType()).isEqualTo(GcsFileSystemOptions.ClientType.HTTP_CLIENT);
    assertThat(options.getReadThreadCount()).isEqualTo(16);
  }
}
