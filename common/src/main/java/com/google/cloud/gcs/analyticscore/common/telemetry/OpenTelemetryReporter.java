/*
 * Copyright 2026 Google LLC
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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A telemetry reporter that records operations and their metrics using OpenTelemetry. */
public class OpenTelemetryReporter implements OperationListener {

  private static final String INSTRUMENTATION_SCOPE_NAME = "com.google.cloud.gcs.analyticscore";

  private final Meter meter;
  private final Map<String, LongHistogram> histograms = new ConcurrentHashMap<>();
  private final Map<String, LongCounter> counters = new ConcurrentHashMap<>();
  private final OpenTelemetryProvider openTelemetryProvider;

  public OpenTelemetryReporter(OpenTelemetryOptions options) {
    switch (options.getProviderType()) {
      case GLOBAL:
        openTelemetryProvider = GlobalOpenTelemetry::get;
        break;
      case LOGGING:
        openTelemetryProvider = new LoggingOpenTelemetryProvider(options);
        break;
      case PRE_CONFIGURED:
        OpenTelemetry otel =
            options
                .getPreconfiguredOpenTelemetryInstance()
                .orElseThrow(() -> new IllegalStateException("Missing OpenTelemetry instance"));
        openTelemetryProvider = () -> otel;
        break;
      default:
        throw new IllegalArgumentException("Unknown ProviderType: " + options.getProviderType());
    }

    this.meter = openTelemetryProvider.getOpenTelemetry().getMeter(INSTRUMENTATION_SCOPE_NAME);
  }

  @Override
  public void close() {
    try {
      openTelemetryProvider.close();
    } catch (Exception e) {
      throw new RuntimeException("Failed to close OpenTelemetryProvider", e);
    }
  }

  @Override
  public void onOperationStart(Operation operation) {}

  @Override
  public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
    if (metrics == null || metrics.isEmpty()) {
      return;
    }
    Attributes operationAttributes = toOpenTelemetryAttributes(operation.getAttributes());
    for (Map.Entry<MetricKey, Long> entry : metrics.entrySet()) {
      MetricKey metricKey = entry.getKey();
      long metricValue = entry.getValue();
      Attributes mergedAttributes =
          Attributes.builder()
              .putAll(operationAttributes)
              .putAll(toOpenTelemetryAttributes(metricKey.getAttributes()))
              .build();
      if (metricKey.getMetric().getType() == Metric.MetricType.DURATION) {
        LongHistogram histogram =
            histograms.computeIfAbsent(
                metricKey.getMetric().getName(),
                name -> meter.histogramBuilder(name).ofLongs().build());
        histogram.record(metricValue, mergedAttributes);
      } else {
        LongCounter counter =
            counters.computeIfAbsent(
                metricKey.getMetric().getName(), name -> meter.counterBuilder(name).build());
        counter.add(metricValue, mergedAttributes);
      }
    }
  }

  private Attributes toOpenTelemetryAttributes(Map<String, String> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return Attributes.empty();
    }
    AttributesBuilder builder = Attributes.builder();
    for (Map.Entry<String, String> entry : attributes.entrySet()) {
      builder.put(AttributeKey.stringKey(entry.getKey()), entry.getValue());
    }
    return builder.build();
  }
}
