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

import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;

class LoggingOpenTelemetryProviderTest {

  @Test
  void testGet_returnsNonNullInstance() {
    try (LoggingOpenTelemetryProvider provider =
        new LoggingOpenTelemetryProvider(OpenTelemetryOptions.builder().build())) {
      OpenTelemetry openTelemetry = provider.getOpenTelemetry();

      assertThat(openTelemetry).isNotNull();
    }
  }

  @Test
  void testGet_returnsSameInstanceOnMultipleCalls() {
    try (LoggingOpenTelemetryProvider provider =
        new LoggingOpenTelemetryProvider(OpenTelemetryOptions.builder().build())) {
      OpenTelemetry firstCall = provider.getOpenTelemetry();
      OpenTelemetry secondCall = provider.getOpenTelemetry();

      assertThat(firstCall).isSameInstanceAs(secondCall);
    }
  }

  @Test
  void testConstructor_withDuration_createsSuccessfully() {
    try (LoggingOpenTelemetryProvider provider =
        new LoggingOpenTelemetryProvider(
            OpenTelemetryOptions.builder().setExportIntervalSeconds(30).build())) {
      OpenTelemetry openTelemetry = provider.getOpenTelemetry();

      assertThat(openTelemetry).isNotNull();
    }
  }
}
