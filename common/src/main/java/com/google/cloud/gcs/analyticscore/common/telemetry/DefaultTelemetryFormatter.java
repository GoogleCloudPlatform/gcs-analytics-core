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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Default implementation of TelemetryFormatter. */
public class DefaultTelemetryFormatter implements TelemetryFormatter {

  @Override
  public String formatOperationStart(Operation operation) {
    return String.format("Operation %s started", operation.getName());
  }

  @Override
  public String formatOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
    String metricsString = formatMetrics(metrics);
    return String.format("Operation %s: %s", operation.getName(), metricsString);
  }

  private String formatMetrics(Map<MetricKey, Long> metrics) {
    return metrics.entrySet().stream()
        .map(
            entry -> {
              MetricKey key = entry.getKey();
              String keyString = key.getName();
              Map<String, String> attributes = key.getAttributes();
              List<String> attrs = new ArrayList<>();
              attributes.forEach((k, v) -> attrs.add(k + "=" + v));

              if (!attrs.isEmpty()) {
                keyString += "{" + String.join(", ", attrs) + "}";
              }
              return keyString + "=" + entry.getValue();
            })
        .collect(java.util.stream.Collectors.joining(", "));
  }
}
