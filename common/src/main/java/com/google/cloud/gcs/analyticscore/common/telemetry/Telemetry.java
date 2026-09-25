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

import static com.google.common.base.Preconditions.checkNotNull;

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
   * Stand-in operation for metrics recorded outside any operation scope. It is immutable and
   * identical on every call, so it is built once rather than per metric: {@link #recordMetric} sits
   * on the cache-hit path. A generated id would be misleading here anyway, since there is no
   * operation for it to correlate.
   */
  private static final Operation UNKNOWN_OPERATION =
      Operation.builder().setName("UNKNOWN").setOperationId("UNKNOWN").build();

  private final List<OperationListener> listeners = new CopyOnWriteArrayList<>();

  /**
   * Returns a {@link Telemetry} appropriate for {@code listeners}, which is a no-op implementation
   * when the list is empty.
   *
   * <p>Prefer this to the constructor. {@code measure} wraps every {@code read}, {@code seek} and
   * {@code write}, and this class does the work of collecting metrics whether or not anything is
   * listening; asking "is anyone listening?" once, here, keeps that question off the data plane.
   */
  public static Telemetry create(List<OperationListener> listeners) {
    checkNotNull(listeners, "listeners cannot be null");
    return listeners.isEmpty() ? new NoOpTelemetry() : new Telemetry(listeners);
  }

  /**
   * Prefer {@link #create(List)}, which selects a no-op implementation when there are no listeners.
   * Constructing directly with an empty list yields an instance that collects metrics nobody will
   * read.
   */
  public Telemetry(List<OperationListener> listeners) {
    checkNotNull(listeners, "listeners cannot be null");
    this.listeners.addAll(listeners);
  }

  /**
   * Executes an operation with telemetry tracking.
   *
   * <p>Callers that have no listener configured should obtain their instance from {@link
   * #create(List)}, which returns an implementation that skips all of this rather than collecting
   * metrics that nothing consumes. {@code measure} wraps every {@code read}, {@code seek} and
   * {@code write}, so that cost would otherwise land on the data plane.
   */
  public <T, E extends Throwable> T measure(
      Operation operation, OperationSupplier<T, E> operationSupplier) throws E {
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
