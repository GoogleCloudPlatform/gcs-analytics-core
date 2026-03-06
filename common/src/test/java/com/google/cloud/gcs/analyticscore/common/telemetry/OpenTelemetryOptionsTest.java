/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.gcs.analyticscore.common.telemetry;

import static com.google.common.truth.Truth.assertThat;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class OpenTelemetryOptionsTest {

  @Test
  void testOpenTelemetryOptionsDefaultValues() {
    OpenTelemetryOptions options = OpenTelemetryOptions.builder().build();
    assertThat(options.isEnabled()).isFalse();
    assertThat(options.getProviderType()).isEqualTo(OpenTelemetryOptions.ProviderType.GLOBAL);
  }

  @Test
  void testOpenTelemetryOptionsCustomValues() {
    OpenTelemetry customTelemetry = GlobalOpenTelemetry.get();
    OpenTelemetryOptions options =
        OpenTelemetryOptions.builder()
            .setEnabled(true)
            .setProviderType(OpenTelemetryOptions.ProviderType.PRE_CONFIGURED)
            .setPreconfiguredOpenTelemetryInstance(customTelemetry)
            .build();

    assertThat(options.isEnabled()).isTrue();
    assertThat(options.getProviderType())
        .isEqualTo(OpenTelemetryOptions.ProviderType.PRE_CONFIGURED);
    assertThat(
            options.getPreconfiguredOpenTelemetryInstance().orElseThrow(IllegalStateException::new))
        .isSameInstanceAs(customTelemetry);
  }

  @Test
  void testOpenTelemetryOptionsLoggingProvider() {
    OpenTelemetryOptions options =
        OpenTelemetryOptions.builder()
            .setEnabled(true)
            .setProviderType(OpenTelemetryOptions.ProviderType.LOGGING)
            .build();

    assertThat(options.isEnabled()).isTrue();
    assertThat(options.getProviderType()).isEqualTo(OpenTelemetryOptions.ProviderType.LOGGING);
  }

  @Test
  void testCreateFromOptions_NoOptions() {
    Map<String, String> options = new HashMap<>();
    Optional<OpenTelemetryOptions> telemetryOptions =
        OpenTelemetryOptions.createFromOptions(options, "prefix.");

    assertThat(telemetryOptions).isEmpty();
  }

  @Test
  void testCreateFromOptions_WithAllOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("prefix.telemetry.opentelemetry.enabled", "true");
    options.put("prefix.telemetry.opentelemetry.provider-type", "PRE_CONFIGURED");
    options.put("prefix.telemetry.opentelemetry.export-interval-seconds", "120");

    Optional<OpenTelemetryOptions> telemetryOptions =
        OpenTelemetryOptions.createFromOptions(options, "prefix.");

    assertThat(telemetryOptions).isPresent();
    assertThat(telemetryOptions.get().isEnabled()).isTrue();
    assertThat(telemetryOptions.get().getProviderType())
        .isEqualTo(OpenTelemetryOptions.ProviderType.PRE_CONFIGURED);
    assertThat(telemetryOptions.get().getExportIntervalSeconds()).isEqualTo(120);
  }
}
