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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Telemetry implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(Telemetry.class);

  private final List<OperationListener> listeners = new CopyOnWriteArrayList<>();

  public Telemetry(List<OperationListener> listeners) {
    this.listeners.addAll(listeners);
  }

  /** Executes an operation with telemetry tracking. */
  public <T, E extends Throwable> T measure(
      Operation operation, OperationSupplier<T, E> operationSupplier) throws E {
    Map<MetricKey, Long> currentMetrics = new ConcurrentHashMap<>();
    MetricsRecorder recorder =
        (name, value, attributes) -> {
          MetricKey key = MetricKey.builder().setName(name).setAttributes(attributes).build();
          currentMetrics.merge(key, value, Long::sum);
        };
    notifyStart(operation);
    long startTime = System.nanoTime();
    try {
      return operationSupplier.get(recorder);
    } finally {
      long durationNs = System.nanoTime() - startTime;
      if (operation.getDurationMetricName().isPresent()) {
        currentMetrics.put(
            MetricKey.builder().setName(operation.getDurationMetricName().get()).build(),
            durationNs);
      }
      notifyEnd(operation, currentMetrics);
    }
  }

  public <T, E extends Throwable> T measure(
      String operationId,
      String operationName,
      String durationMetricName,
      Map<String, String> operationAttributes,
      OperationSupplier<T, E> operationSupplier)
      throws E {
    Operation operation =
        Operation.builder()
            .setOperationId(operationId)
            .setName(operationName)
            .setDurationMetricName(durationMetricName)
            .setAttributes(operationAttributes)
            .build();
    return measure(operation, operationSupplier);
  }

  public <T, E extends Throwable> T measure(
      String operationName,
      String durationMetricName,
      Map<String, String> operationAttributes,
      OperationSupplier<T, E> operationSupplier)
      throws E {
    Operation operation =
        Operation.builder()
            .setName(operationName)
            .setDurationMetricName(durationMetricName)
            .setAttributes(operationAttributes)
            .build();
    return measure(operation, operationSupplier);
  }

  /**
   * Records metric that is not associated with any specific operation context. This is useful for
   * interceptors or background processes where no operation scope is available.
   */
  public void recordMetric(String name, long value, Map<String, String> attributes) {
    notifyEnd(
        Operation.builder().setName("UNKNOWN").build(),
        Collections.singletonMap(
            MetricKey.builder().setName(name).setAttributes(attributes).build(), value));
  }

  private void notifyStart(Operation operation) {
    for (OperationListener listener : listeners) {
      try {
        listener.onOperationStart(operation);
      } catch (Exception e) {
        LOG.info("Exception in notifyStart for listener {}", listener.getClass().getName(), e);
      }
    }
  }

  private void notifyEnd(Operation operation, Map<MetricKey, Long> metrics) {
    for (OperationListener listener : listeners) {
      try {
        listener.onOperationEnd(operation, metrics);
      } catch (Exception e) {
        LOG.info("Exception in notifyEnd for listener {}", listener.getClass().getName(), e);
      }
    }
  }

  @Override
  public void close() {
    listeners.clear();
  }
}
