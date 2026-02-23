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

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class LoggingTelemetryReporterTest {

  @Test
  public void testLoggingOptionsDefaultValues() {
    LoggingTelemetryOptions options = LoggingTelemetryOptions.builder().build();

    assertThat(options.isEnabled()).isFalse();
    assertThat(options.getLogLevel()).isEqualTo(LoggingTelemetryOptions.LogLevel.DEBUG);
  }

  @Test
  public void testLoggingOptionsCustomValues() {
    LoggingTelemetryOptions options =
        LoggingTelemetryOptions.builder()
            .setEnabled(true)
            .setLogLevel(LoggingTelemetryOptions.LogLevel.INFO)
            .build();

    assertThat(options.isEnabled()).isTrue();
    assertThat(options.getLogLevel()).isEqualTo(LoggingTelemetryOptions.LogLevel.INFO);
  }

  @Test
  public void testFormatMetrics_singleMetricWithoutAttributes() {
    LoggingTelemetryReporter reporter =
        new LoggingTelemetryReporter(LoggingTelemetryOptions.builder().build());
    Map<MetricKey, Long> metrics = Map.of(MetricKey.builder().setName("TestMetric").build(), 100L);

    String formattedMetrics = reporter.formatMetrics(metrics);

    assertThat(formattedMetrics).isEqualTo("{TestMetric=100}");
  }

  @Test
  public void testFormatMetrics_singleMetricWithAttributes() {
    LoggingTelemetryReporter reporter =
        new LoggingTelemetryReporter(LoggingTelemetryOptions.builder().build());
    Map<MetricKey, Long> metrics =
        Map.of(
            MetricKey.builder()
                .setName("TestMetric")
                .setAttributes(Map.of("key1", "value1", "key2", "value2"))
                .build(),
            100L);

    String formattedMetrics = reporter.formatMetrics(metrics);

    assertThat(formattedMetrics)
        .isAnyOf(
            "{TestMetric{key1=value1, key2=value2}=100}",
            "{TestMetric{key2=value2, key1=value1}=100}");
  }

  @Test
  public void testFormatMetrics_multipleMetrics() {
    LoggingTelemetryReporter reporter =
        new LoggingTelemetryReporter(LoggingTelemetryOptions.builder().build());
    Map<MetricKey, Long> metrics =
        Map.of(
            MetricKey.builder().setName("Metric1").build(),
            100L,
            MetricKey.builder().setName("Metric2").setAttributes(Map.of("key", "value")).build(),
            200L);

    String formattedMetrics = reporter.formatMetrics(metrics);

    assertThat(formattedMetrics)
        .isAnyOf("{Metric1=100, Metric2{key=value}=200}", "{Metric2{key=value}=200, Metric1=100}");
  }
}
