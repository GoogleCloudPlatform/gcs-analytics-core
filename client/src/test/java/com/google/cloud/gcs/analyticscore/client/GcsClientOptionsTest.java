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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.gcs.analyticscore.client.GcsClientOptions.ClientType;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class GcsClientOptionsTest {

  @Test
  void createFromOptions_withValidProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            "fs.gs.project-id", "test-project",
            "fs.gs.client.type", "GRPC_CLIENT",
            "fs.gs.client.grpc.direct-path-enabled", "false",
            "fs.gs.client-lib-token", "test-token",
            "fs.gs.service.host", "test-host",
            "fs.gs.user-agent", "test-agent");

    GcsClientOptions options = GcsClientOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getProjectId()).hasValue("test-project");
    assertThat(options.getClientType()).isEqualTo(ClientType.GRPC_CLIENT);
    assertThat(options.isDirectPathEnabled()).isFalse();
    assertThat(options.getClientLibToken()).hasValue("test-token");
    assertThat(options.getServiceHost()).hasValue("test-host");
    assertThat(options.getUserAgent()).hasValue("test-agent");
  }

  @Test
  void createFromOptions_withDefaultProperties_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties = ImmutableMap.of();

    GcsClientOptions options = GcsClientOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getProjectId()).isEmpty();
    assertThat(options.getClientType()).isEqualTo(ClientType.HTTP_CLIENT);
    assertThat(options.isDirectPathEnabled()).isTrue();
    assertThat(options.getClientLibToken()).isEmpty();
    assertThat(options.getServiceHost()).isEmpty();
    assertThat(options.getUserAgent()).isEmpty();
  }

  @Test
  void createFromOptions_withCaseInsensitiveClientType_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties = ImmutableMap.of("fs.gs.client.type", "gRpC_ClIeNt");

    GcsClientOptions options = GcsClientOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getClientType()).isEqualTo(ClientType.GRPC_CLIENT);
  }

  @Test
  void createFromOptions_withWhitespaceClientType_shouldCreateCorrectOptions() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of("fs.gs.client.type", "  GRPC_CLIENT  ");

    GcsClientOptions options = GcsClientOptions.createFromOptions(properties, "fs.gs.");

    assertThat(options.getClientType()).isEqualTo(ClientType.GRPC_CLIENT);
  }
}
