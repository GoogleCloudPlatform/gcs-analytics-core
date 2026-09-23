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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;

@ExtendWith(MockitoExtension.class)
class LoggingTelemetryReporterTest {

  private static LoggingTelemetryOptions optionsAt(LoggingTelemetryOptions.LogLevel logLevel) {
    return LoggingTelemetryOptions.builder().setLogLevel(logLevel).build();
  }

  @Test
  void formatMetrics_singleMetricWithoutAttributes() {
    try (LoggingTelemetryReporter reporter =
        new LoggingTelemetryReporter(LoggingTelemetryOptions.builder().build())) {
      Map<MetricKey, Long> metrics =
          Map.of(
              MetricKey.builder()
                  .setMetric(TestMetric.of("TestMetric", Metric.MetricType.COUNTER))
                  .build(),
              100L);

      String formattedMetrics = reporter.formatMetrics(metrics);

      assertThat(formattedMetrics).isEqualTo("{TestMetric=100}");
    }
  }

  @Test
  void formatMetrics_singleMetricWithAttributes() {
    try (LoggingTelemetryReporter reporter =
        new LoggingTelemetryReporter(LoggingTelemetryOptions.builder().build())) {
      Map<MetricKey, Long> metrics =
          Map.of(
              MetricKey.builder()
                  .setMetric(TestMetric.of("TestMetric", Metric.MetricType.COUNTER))
                  .setAttributes(Map.of("key1", "value1", "key2", "value2"))
                  .build(),
              100L);

      String formattedMetrics = reporter.formatMetrics(metrics);

      assertThat(formattedMetrics)
          .isAnyOf(
              "{TestMetric{key1=value1, key2=value2}=100}",
              "{TestMetric{key2=value2, key1=value1}=100}");
    }
  }

  @Test
  void formatMetrics_multipleMetrics() {
    try (LoggingTelemetryReporter reporter =
        new LoggingTelemetryReporter(LoggingTelemetryOptions.builder().build())) {
      Map<MetricKey, Long> metrics =
          Map.of(
              MetricKey.builder()
                  .setMetric(TestMetric.of("Metric1", Metric.MetricType.COUNTER))
                  .build(),
              100L,
              MetricKey.builder()
                  .setMetric(TestMetric.of("Metric2", Metric.MetricType.COUNTER))
                  .setAttributes(Map.of("key", "value"))
                  .build(),
              200L);

      String formattedMetrics = reporter.formatMetrics(metrics);

      assertThat(formattedMetrics)
          .isAnyOf(
              "{Metric1=100, Metric2{key=value}=200}", "{Metric2{key=value}=200, Metric1=100}");
    }
  }

  @Test
  void onOperationStart_levelEnabled_logsOperationDetails() {
    RecordingLogger logger = RecordingLogger.enabled();
    LoggingTelemetryReporter reporter =
        new LoggingTelemetryReporter(optionsAt(LoggingTelemetryOptions.LogLevel.INFO), logger);
    Operation operation =
        Operation.builder()
            .setName("READ")
            .setOperationId("op-1")
            .setAttributes(Map.of("bucket", "b"))
            .build();

    reporter.onOperationStart(operation);

    assertThat(logger.getMessages())
        .containsExactly("Operation started: [READ], id: [op-1], attributes: {bucket=b}");
  }

  @Test
  void onOperationEnd_levelEnabled_logsOperationDetailsAndMetrics() {
    RecordingLogger logger = RecordingLogger.enabled();
    LoggingTelemetryReporter reporter =
        new LoggingTelemetryReporter(optionsAt(LoggingTelemetryOptions.LogLevel.INFO), logger);
    Operation operation = Operation.builder().setName("READ").setOperationId("op-1").build();
    Map<MetricKey, Long> metrics =
        Map.of(
            MetricKey.builder()
                .setMetric(TestMetric.of("Metric1", Metric.MetricType.COUNTER))
                .build(),
            100L);

    reporter.onOperationEnd(operation, metrics);

    assertThat(logger.getMessages())
        .containsExactly(
            "Operation ended: [READ], id: [op-1], attributes: {}, metrics: {Metric1=100}");
  }

  @ParameterizedTest
  @CsvSource({"TRACE,TRACE", "DEBUG,DEBUG", "INFO,INFO", "WARNING,WARN", "ERROR,ERROR"})
  void onOperationStart_everyConfiguredLevel_emitsAtTheMatchingSlf4jLevel(
      LoggingTelemetryOptions.LogLevel configured, Level expected) {
    RecordingLogger logger = RecordingLogger.enabled();
    LoggingTelemetryReporter reporter = new LoggingTelemetryReporter(optionsAt(configured), logger);

    reporter.onOperationStart(Operation.builder().setName("READ").build());

    assertThat(logger.getLevels()).containsExactly(expected);
  }

  @ParameterizedTest
  @CsvSource({"TRACE,TRACE", "DEBUG,DEBUG", "INFO,INFO", "WARNING,WARN", "ERROR,ERROR"})
  void onOperationEnd_everyConfiguredLevel_emitsAtTheMatchingSlf4jLevel(
      LoggingTelemetryOptions.LogLevel configured, Level expected) {
    RecordingLogger logger = RecordingLogger.enabled();
    LoggingTelemetryReporter reporter = new LoggingTelemetryReporter(optionsAt(configured), logger);

    reporter.onOperationEnd(Operation.builder().setName("READ").build(), Map.of());

    assertThat(logger.getLevels()).containsExactly(expected);
  }

  @ParameterizedTest
  @EnumSource(LoggingTelemetryOptions.LogLevel.class)
  void onOperationEnd_levelDisabled_logsNothing(LoggingTelemetryOptions.LogLevel logLevel) {
    RecordingLogger logger = RecordingLogger.disabled();
    LoggingTelemetryReporter reporter = new LoggingTelemetryReporter(optionsAt(logLevel), logger);

    reporter.onOperationEnd(Operation.builder().setName("READ").build(), Map.of());

    assertThat(logger.getMessages()).isEmpty();
  }

  @Test
  void onOperationEnd_levelDisabled_doesNotFormatMetrics() {
    AtomicInteger formatCalls = new AtomicInteger();
    LoggingTelemetryReporter reporter =
        new LoggingTelemetryReporter(
            optionsAt(LoggingTelemetryOptions.LogLevel.INFO), RecordingLogger.disabled()) {
          @Override
          String formatMetrics(Map<MetricKey, Long> metrics) {
            formatCalls.incrementAndGet();
            return super.formatMetrics(metrics);
          }
        };

    reporter.onOperationEnd(Operation.builder().setName("READ").build(), Map.of());

    // The saving this reporter relies on: the message is never rendered when nobody will read it.
    assertThat(formatCalls.get()).isEqualTo(0);
  }
}
