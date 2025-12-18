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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class GcsAnalyticsCoreTelemetry {

  private static final GcsAnalyticsCoreTelemetry INSTANCE = new GcsAnalyticsCoreTelemetry();
  private final List<GcsOperationMetricsListener> listeners = new CopyOnWriteArrayList<>();

  public static GcsAnalyticsCoreTelemetry getInstance() {
    return INSTANCE;
  }

  public void addListener(GcsOperationMetricsListener listener) {
    listeners.add(listener);
  }

  public void removeListener(GcsOperationMetricsListener listener) {
    listeners.remove(listener);
  }

  /** Executes an operation with telemetry tracking. */
  public <T, E extends Throwable> T measure(
      GcsOperation operation, OperationSupplier<T, E> operationSupplier) throws E {
    Map<MetricKey, Long> currentMetrics = new ConcurrentHashMap<>();
    MetricsRecorder recorder =
        (name, value, attributes) -> {
          MetricKey key = MetricKey.builder().setName(name).setAttributes(attributes).build();
          currentMetrics.merge(key, value, Long::sum);
        };
    long startTime = System.nanoTime();
    notifyStart(operation);
    try {
      return operationSupplier.get(recorder);
    } finally {
      long durationNs = System.nanoTime() - startTime;
      operation
          .getDurationMetricName()
          .ifPresent(
              durationMetric ->
                  currentMetrics.put(
                      MetricKey.builder().setName(durationMetric).build(), durationNs));
      notifyEnd(operation, currentMetrics);
    }
  }

  /**
   * Records metric that is not associated with any specific operation context. This is useful for
   * interceptors or background processes where no operation scope is available.
   */
  public void recordMetric(String name, long value, TelemetryAttributes attributes) {
    notifyEnd(
        GcsOperation.builder().setType(GcsOperationType.NA).build(),
        Collections.singletonMap(
            MetricKey.builder().setName(name).setAttributes(attributes).build(), value));
  }

  private void notifyStart(GcsOperation operation) {
    for (GcsOperationMetricsListener listener : listeners) {
      listener.onOperationStart(operation);
    }
  }

  private void notifyEnd(GcsOperation operation, Map<MetricKey, Long> metrics) {
    for (GcsOperationMetricsListener listener : listeners) {
      listener.onOperationEnd(operation, metrics);
    }
  }
}
