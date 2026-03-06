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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LoggingTelemetryOptionsTest {

  @Test
  void testLoggingTelemetryOptionsDefaultValues() {
    LoggingTelemetryOptions options = LoggingTelemetryOptions.builder().build();

    assertThat(options.isEnabled()).isFalse();
    assertThat(options.getLogLevel()).isEqualTo(LoggingTelemetryOptions.LogLevel.DEBUG);
  }

  @Test
  void testCreateFromOptions_NoOptions() {
    Map<String, String> options = new HashMap<>();
    Optional<LoggingTelemetryOptions> telemetryOptions =
        LoggingTelemetryOptions.createFromOptions(options, "prefix.");

    assertThat(telemetryOptions).isEmpty();
  }

  @Test
  void testCreateFromOptions_WithAllOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("prefix.telemetry.logging.enabled", "true");
    options.put("prefix.telemetry.logging.level", "ERROR");

    Optional<LoggingTelemetryOptions> telemetryOptions =
        LoggingTelemetryOptions.createFromOptions(options, "prefix.");

    assertThat(telemetryOptions).isPresent();
    assertThat(telemetryOptions.get().isEnabled()).isTrue();
    assertThat(telemetryOptions.get().getLogLevel())
        .isEqualTo(LoggingTelemetryOptions.LogLevel.ERROR);
  }

  @Test
  void testCreateFromOptions_WithInvalidLevel_fallsbackToDefaults() {
    Map<String, String> options = new HashMap<>();
    options.put("prefix.telemetry.logging.enabled", "true");
    options.put("prefix.telemetry.logging.level", "INVALID_LEVEL");

    Optional<LoggingTelemetryOptions> telemetryOptions =
        LoggingTelemetryOptions.createFromOptions(options, "prefix.");

    assertThat(telemetryOptions).isPresent();
    assertThat(telemetryOptions.get().isEnabled()).isTrue();
    assertThat(telemetryOptions.get().getLogLevel())
        .isEqualTo(LoggingTelemetryOptions.LogLevel.DEBUG);
  }
}
