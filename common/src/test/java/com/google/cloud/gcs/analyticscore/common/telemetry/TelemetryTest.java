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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TelemetryTest {

  private Telemetry telemetry;
  private FakeOperationMetricsListener listener;

  @BeforeEach
  void setUp() {
    listener = new FakeOperationMetricsListener();
    telemetry = new Telemetry(Collections.singletonList(listener));
  }

  @Test
  void measure_validOperation_returnsResultAndRecordsMetrics() throws Exception {
    Metric durationMetric = TestMetric.of("duration", Metric.MetricType.DURATION);
    Operation operation =
        Operation.builder().setName("READ").setDurationMetric(durationMetric).build();

    String result =
        telemetry.measure(
            operation.getName(),
            operation.getDurationMetric().orElse(null),
            operation.getAttributes(),
            recorder -> "result");

    Map<MetricKey, Long> metrics = listener.getEndedMetrics().get(0);
    Operation startedOp = listener.getStartedOperations().get(0);
    Operation endedOp = listener.getEndedOperations().get(0);
    assertThat(result).isEqualTo("result");
    assertThat(listener.getStartedOperations()).hasSize(1);
    assertThat(startedOp.getName()).isEqualTo(operation.getName());
    assertThat(startedOp.getAttributes()).isEqualTo(operation.getAttributes());
    assertThat(startedOp.getDurationMetric()).isEqualTo(operation.getDurationMetric());
    assertThat(listener.getEndedOperations()).hasSize(1);
    assertThat(endedOp.getName()).isEqualTo(operation.getName());
    assertThat(endedOp.getAttributes()).isEqualTo(operation.getAttributes());
    assertThat(endedOp.getDurationMetric()).isEqualTo(operation.getDurationMetric());
    assertThat(listener.getEndedMetrics()).hasSize(1);
    assertThat(
            metrics.keySet().stream().anyMatch(key -> "duration".equals(key.getMetric().getName())))
        .isTrue();
  }

  @Test
  void recordMetric_validInput_recordsMetricWithTypeNA() {
    Metric testMetric = TestMetric.of("testMetric", Metric.MetricType.COUNTER);
    long value = 123L;
    Map<String, String> attributes = Collections.emptyMap();

    telemetry.recordMetric(testMetric, value, attributes);

    Map<MetricKey, Long> metrics = listener.getEndedMetrics().get(0);
    MetricKey key = metrics.keySet().iterator().next();
    assertThat(listener.getEndedOperations()).hasSize(1);
    assertThat(listener.getEndedOperations().get(0).getName()).isEqualTo("UNKNOWN");
    assertThat(metrics).hasSize(1);
    assertThat(key.getMetric().getName()).isEqualTo("testMetric");
    assertThat(key.getAttributes()).isEqualTo(attributes);
    assertThat(metrics.get(key)).isEqualTo(value);
  }

  @Test
  void recordMetric_calledRepeatedly_reportsTheUnknownOperationId() {
    Metric testMetric = TestMetric.of("testMetric", Metric.MetricType.COUNTER);

    telemetry.recordMetric(testMetric, 1L, Collections.emptyMap());
    telemetry.recordMetric(testMetric, 2L, Collections.emptyMap());

    // A per-call id would be pure garbage: there is no operation for it to correlate with.
    assertThat(listener.getEndedOperations().stream().map(Operation::getOperationId))
        .containsExactly("UNKNOWN", "UNKNOWN");
  }

  @Test
  void measure_withoutListeners_returnsSupplierResult() throws Exception {
    Telemetry telemetryWithoutListeners = new Telemetry(Collections.emptyList());

    String result =
        telemetryWithoutListeners.measure(
            "READ",
            TestMetric.of("duration", Metric.MetricType.DURATION),
            Collections.emptyMap(),
            recorder -> "result");

    assertThat(result).isEqualTo("result");
  }

  @Test
  void measure_withoutListeners_acceptsRecordedMetrics() throws Exception {
    Telemetry telemetryWithoutListeners = new Telemetry(Collections.emptyList());
    Metric counter = TestMetric.of("bytes", Metric.MetricType.COUNTER);

    String result =
        telemetryWithoutListeners.measure(
            "READ",
            TestMetric.of("duration", Metric.MetricType.DURATION),
            Collections.emptyMap(),
            recorder -> {
              recorder.record(counter, 42L, Collections.emptyMap());
              return "result";
            });

    assertThat(result).isEqualTo("result");
  }

  @Test
  void measure_withoutListeners_propagatesSupplierException() {
    Telemetry telemetryWithoutListeners = new Telemetry(Collections.emptyList());
    IllegalStateException thrown = new IllegalStateException("boom");

    IllegalStateException actual =
        assertThrows(
            IllegalStateException.class,
            () ->
                telemetryWithoutListeners.measure(
                    "READ",
                    TestMetric.of("duration", Metric.MetricType.DURATION),
                    Collections.emptyMap(),
                    recorder -> {
                      throw thrown;
                    }));

    assertThat(actual).isSameInstanceAs(thrown);
  }

  @Test
  void measure_withExplicitOperationAndWithoutListeners_returnsSupplierResult() throws Exception {
    Telemetry telemetryWithoutListeners = new Telemetry(Collections.emptyList());
    Operation operation = Operation.builder().setName("READ").build();

    String result = telemetryWithoutListeners.measure(operation, recorder -> "result");

    assertThat(result).isEqualTo("result");
  }

  @Test
  void measure_withExplicitOperationIdAndWithoutListeners_returnsSupplierResult() throws Exception {
    Telemetry telemetryWithoutListeners = new Telemetry(Collections.emptyList());

    String result =
        telemetryWithoutListeners.measure(
            "operation-id",
            "READ",
            TestMetric.of("duration", Metric.MetricType.DURATION),
            Collections.emptyMap(),
            recorder -> "result");

    assertThat(result).isEqualTo("result");
  }

  @Test
  void measure_afterClose_doesNotNotifyListener() throws Exception {
    // close() clears the listener list, which is the observable way to reach the no-listener path
    // while still holding a reference to the listener that would otherwise have been notified.
    telemetry.close();

    Object unused =
        telemetry.measure(
            "READ",
            TestMetric.of("duration", Metric.MetricType.DURATION),
            Collections.emptyMap(),
            recorder -> {
              recorder.record(
                  TestMetric.of("bytes", Metric.MetricType.COUNTER), 42L, Collections.emptyMap());
              return "result";
            });

    assertThat(listener.getStartedOperations()).isEmpty();
  }

  @Test
  void recordMetric_afterClose_doesNotNotifyListener() {
    telemetry.close();

    telemetry.recordMetric(
        TestMetric.of("testMetric", Metric.MetricType.COUNTER), 1L, Collections.emptyMap());

    assertThat(listener.getEndedOperations()).isEmpty();
  }

  @Test
  void measure_withExplicitOperationId_propagatesIdToListener() throws Exception {
    Object unused =
        telemetry.measure(
            "explicit-operation-id",
            "READ",
            TestMetric.of("duration", Metric.MetricType.DURATION),
            Collections.emptyMap(),
            recorder -> "result");

    assertThat(listener.getStartedOperations().get(0).getOperationId())
        .isEqualTo("explicit-operation-id");
  }

  @Test
  void measure_listenerThrowsOnStart_stillReturnsSupplierResult() throws Exception {
    Telemetry telemetryWithFailingListener =
        new Telemetry(Collections.singletonList(new ThrowingOperationListener()));

    String result =
        telemetryWithFailingListener.measure(
            "READ",
            TestMetric.of("duration", Metric.MetricType.DURATION),
            Collections.emptyMap(),
            recorder -> "result");

    assertThat(result).isEqualTo("result");
  }

  @Test
  void recordMetric_listenerThrows_doesNotPropagate() {
    Telemetry telemetryWithFailingListener =
        new Telemetry(Collections.singletonList(new ThrowingOperationListener()));
    Metric testMetric = TestMetric.of("testMetric", Metric.MetricType.COUNTER);

    assertDoesNotThrow(
        () -> telemetryWithFailingListener.recordMetric(testMetric, 1L, Collections.emptyMap()));
  }

  /** Fails on every callback, to verify that a misbehaving listener cannot break an operation. */
  private static final class ThrowingOperationListener implements OperationListener {
    @Override
    public void onOperationStart(Operation operation) {
      throw new IllegalStateException("listener failed on start");
    }

    @Override
    public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
      throw new IllegalStateException("listener failed on end");
    }
  }
}
