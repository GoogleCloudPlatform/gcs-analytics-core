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

  /**
   * Discards every metric recorded against it. Handed to the supplier when no listener is
   * registered, so that callers always have a usable recorder without anything being retained.
   */
  private static final MetricsRecorder NO_OP_RECORDER = (metric, value, attributes) -> {};

  /**
   * Stand-in operation for metrics recorded outside any operation scope. It is immutable and
   * identical on every call, so it is built once rather than per metric: {@link #recordMetric} sits
   * on the cache-hit path. A generated id would be misleading here anyway, since there is no
   * operation for it to correlate.
   */
  private static final Operation UNKNOWN_OPERATION =
      Operation.builder().setName("UNKNOWN").setOperationId("UNKNOWN").build();

  private final List<OperationListener> listeners = new CopyOnWriteArrayList<>();

  public Telemetry(List<OperationListener> listeners) {
    this.listeners.addAll(listeners);
  }

  /**
   * Executes an operation with telemetry tracking.
   *
   * <p>When no listener is registered the supplier is invoked directly against {@link
   * #NO_OP_RECORDER}: no metric map, no timing calls and no notifications. {@code measure} wraps
   * every {@code read}, {@code seek} and {@code write}, so without this the cost of collecting
   * metrics that nothing consumes would be paid on the data plane.
   */
  public <T, E extends Throwable> T measure(
      Operation operation, OperationSupplier<T, E> operationSupplier) throws E {
    if (listeners.isEmpty()) {
      return operationSupplier.get(NO_OP_RECORDER);
    }
    // Deliberately concurrent: MetricsRecorder is public API, so a supplier is free to fan work
    // out across threads and record from each of them.
    Map<MetricKey, Long> currentMetrics = new ConcurrentHashMap<>();
    MetricsRecorder recorder =
        (metric, value, attributes) -> {
          MetricKey key = MetricKey.builder().setMetric(metric).setAttributes(attributes).build();
          currentMetrics.merge(key, value, Long::sum);
        };
    notifyStart(operation);
    long startTime = System.nanoTime();
    try {
      return operationSupplier.get(recorder);
    } finally {
      long durationNs = System.nanoTime() - startTime;
      operation
          .getDurationMetric()
          .ifPresent(
              metric ->
                  currentMetrics.put(MetricKey.builder().setMetric(metric).build(), durationNs));
      notifyEnd(operation, currentMetrics);
    }
  }

  public <T, E extends Throwable> T measure(
      String operationId,
      String operationName,
      Metric durationMetric,
      Map<String, String> operationAttributes,
      OperationSupplier<T, E> operationSupplier)
      throws E {
    // Checked before building the Operation: construction is itself allocation we can skip.
    if (listeners.isEmpty()) {
      return operationSupplier.get(NO_OP_RECORDER);
    }
    Operation operation =
        Operation.builder()
            .setOperationId(operationId)
            .setName(operationName)
            .setDurationMetric(durationMetric)
            .setAttributes(operationAttributes)
            .build();
    return measure(operation, operationSupplier);
  }

  public <T, E extends Throwable> T measure(
      String operationName,
      Metric durationMetric,
      Map<String, String> operationAttributes,
      OperationSupplier<T, E> operationSupplier)
      throws E {
    // Checked before building the Operation, which would otherwise generate an operation id.
    if (listeners.isEmpty()) {
      return operationSupplier.get(NO_OP_RECORDER);
    }
    Operation operation =
        Operation.builder()
            .setName(operationName)
            .setDurationMetric(durationMetric)
            .setAttributes(operationAttributes)
            .build();
    return measure(operation, operationSupplier);
  }

  /**
   * Records metric that is not associated with any specific operation context. This is useful for
   * interceptors or background processes where no operation scope is available.
   */
  public void recordMetric(Metric metric, long value, Map<String, String> attributes) {
    if (listeners.isEmpty()) {
      return;
    }
    notifyEnd(
        UNKNOWN_OPERATION,
        Collections.singletonMap(
            MetricKey.builder().setMetric(metric).setAttributes(attributes).build(), value));
  }

  private void notifyStart(Operation operation) {
    for (OperationListener listener : listeners) {
      try {
        listener.onOperationStart(operation);
      } catch (Exception e) {
        LOG.error("Exception in notifyStart for listener {}", listener.getClass().getName(), e);
      }
    }
  }

  private void notifyEnd(Operation operation, Map<MetricKey, Long> metrics) {
    for (OperationListener listener : listeners) {
      try {
        listener.onOperationEnd(operation, metrics);
      } catch (Exception e) {
        LOG.error("Exception in notifyEnd for listener {}", listener.getClass().getName(), e);
      }
    }
  }

  @Override
  public void close() {
    for (OperationListener listener : listeners) {
      listener.close();
    }
    listeners.clear();
  }
}
