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
package com.google.cloud.gcs.analyticscore.common.telemetry;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TelemetryOptionsTest {

  @Test
  public void testBuilderWithCustomTelemetryOptions() {
    OperationListener listener =
        new OperationListener() {
          @Override
          public void onOperationStart(Operation operation) {}

          @Override
          public void onOperationEnd(Operation operation, java.util.Map<MetricKey, Long> metrics) {}
        };
    CustomTelemetryOptions customTelemetryOptions =
        CustomTelemetryOptions.builder().setOperationListeners(ImmutableList.of(listener)).build();
    TelemetryOptions options =
        TelemetryOptions.builder().setCustomTelemetryOptions(customTelemetryOptions).build();

    assertThat(options.getCustomTelemetryOptions().get().getOperationListeners())
        .containsExactly(listener);
  }

  @Test
  public void testCreateFromOptions_Empty() {
    Map<String, String> optionsMap = new HashMap<>();
    TelemetryOptions options = TelemetryOptions.createFromOptions(optionsMap, "prefix.");

    assertThat(options.getLoggingTelemetryOptions()).isEmpty();
    assertThat(options.getOpenTelemetryOptions()).isEmpty();
  }

  @Test
  public void testCreateFromOptions_WithLogging() {
    Map<String, String> optionsMap = new HashMap<>();
    optionsMap.put("prefix.telemetry.logging.enabled", "true");
    optionsMap.put("prefix.telemetry.logging.level", "INFO");

    TelemetryOptions options = TelemetryOptions.createFromOptions(optionsMap, "prefix.");

    assertThat(options.getLoggingTelemetryOptions()).isPresent();
    assertThat(options.getLoggingTelemetryOptions().get().isEnabled()).isTrue();
    assertThat(options.getLoggingTelemetryOptions().get().getLogLevel())
        .isEqualTo(LoggingTelemetryOptions.LogLevel.INFO);
    assertThat(options.getOpenTelemetryOptions()).isEmpty();
  }

  @Test
  public void testCreateFromOptions_WithOpenTelemetry() {
    Map<String, String> optionsMap = new HashMap<>();
    optionsMap.put("prefix.telemetry.opentelemetry.enabled", "true");
    optionsMap.put("prefix.telemetry.opentelemetry.provider-type", "LOGGING");

    TelemetryOptions options = TelemetryOptions.createFromOptions(optionsMap, "prefix.");

    assertThat(options.getOpenTelemetryOptions()).isPresent();
    assertThat(options.getOpenTelemetryOptions().get().isEnabled()).isTrue();
    assertThat(options.getOpenTelemetryOptions().get().getProviderType())
        .isEqualTo(OpenTelemetryOptions.ProviderType.LOGGING);
    assertThat(options.getLoggingTelemetryOptions()).isEmpty();
  }

  @Test
  public void testCreateFromOptions_WithAll() {
    Map<String, String> optionsMap = new HashMap<>();
    optionsMap.put("prefix.telemetry.logging.enabled", "true");
    optionsMap.put("prefix.telemetry.opentelemetry.enabled", "false");

    TelemetryOptions options = TelemetryOptions.createFromOptions(optionsMap, "prefix.");

    assertThat(options.getLoggingTelemetryOptions()).isPresent();
    assertThat(options.getLoggingTelemetryOptions().get().isEnabled()).isTrue();
    assertThat(options.getOpenTelemetryOptions()).isPresent();
    assertThat(options.getOpenTelemetryOptions().get().isEnabled()).isFalse();
  }
}
