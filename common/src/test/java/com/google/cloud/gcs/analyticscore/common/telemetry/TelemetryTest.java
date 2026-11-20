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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TelemetryTest {

  private static final Metric M1 = TestMetric.of("m1", Metric.MetricType.COUNTER);
  private static final Metric M2 = TestMetric.of("m2", Metric.MetricType.COUNTER);
  private static final Metric M3 = TestMetric.of("m3", Metric.MetricType.COUNTER);
  private static final Metric DURATION = TestMetric.of("duration", Metric.MetricType.DURATION);
  private static final MetricKey K1 = keyOf(M1);
  private static final MetricKey K2 = keyOf(M2);
  private static final MetricKey K3 = keyOf(M3);
  private static final MetricKey DURATION_KEY = keyOf(DURATION);
  private static final Operation READ_OP = Operation.builder().setName("READ").build();
  private static final Operation READ_DURATION_OP =
      Operation.builder().setName("READ").setDurationMetric(DURATION).build();

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

  @Test
  void measure_noMetricsAndNoDuration_deliversEmptyUnmodifiableMap() throws Exception {
    Map<MetricKey, Long> metrics = measureMetrics(READ_OP, r -> {});

    assertThat(metrics).isEmpty();
    assertThat(metrics).hasSize(0);
    assertThat(metrics.get(K1)).isNull();
    assertThat(metrics.containsKey(K1)).isFalse();
    assertThat(metrics.entrySet()).isEmpty();
  }

  @Test
  void measure_singleMetricRepeated_accumulatesAndIsUnmodifiable() throws Exception {
    Map<MetricKey, Long> metrics =
        measureMetrics(
            READ_OP,
            r -> {
              rec(r, M1, 10L);
              rec(r, M1, 15L);
            });

    Iterator<Map.Entry<MetricKey, Long>> it = metrics.entrySet().iterator();
    assertThat(metrics).hasSize(1);
    assertThat(metrics.entrySet()).hasSize(1);
    assertThat(metrics.get(K1)).isEqualTo(25L);
    assertThat(metrics.get(K2)).isNull();
    assertThat(metrics.containsKey(K1)).isTrue();
    assertThat(metrics.containsKey(null)).isFalse();
    assertThat(it.hasNext()).isTrue();
    assertThat(it.next()).isEqualTo(Map.entry(K1, 25L));
    assertThat(it.hasNext()).isFalse();
    assertThrows(NoSuchElementException.class, it::next);
    assertThrows(UnsupportedOperationException.class, () -> metrics.put(K2, 1L));
    assertThrows(UnsupportedOperationException.class, () -> metrics.remove(K1));
    assertThrows(UnsupportedOperationException.class, metrics::clear);
  }

  @Test
  void measure_twoMetricsWithSecondRepeated_accumulatesAndMatchesMapEquality() throws Exception {
    Map<MetricKey, Long> metrics =
        measureMetrics(
            READ_OP,
            r -> {
              rec(r, M1, 5L);
              rec(r, M2, 7L);
              rec(r, M2, 3L);
            });

    Map<MetricKey, Long> expected = Map.of(K1, 5L, K2, 10L);
    assertThat(metrics).hasSize(2);
    assertThat(metrics.get(K1)).isEqualTo(5L);
    assertThat(metrics.get(K2)).isEqualTo(10L);
    assertThat(metrics.get(K3)).isNull();
    assertThat(metrics).isEqualTo(expected);
    assertThat(metrics.hashCode()).isEqualTo(expected.hashCode());
  }

  @Test
  void measure_moreThanTwoMetrics_spillsToOverflowAndRemainsUnmodifiable() throws Exception {
    Map<MetricKey, Long> metrics =
        measureMetrics(
            READ_DURATION_OP,
            r -> {
              rec(r, M1, 1L);
              rec(r, M2, 2L);
              rec(r, M3, 3L);
              rec(r, M3, 4L);
            });

    Map.Entry<MetricKey, Long> firstEntry = metrics.entrySet().iterator().next();
    assertThat(metrics).hasSize(4);
    assertThat(metrics.get(K1)).isEqualTo(1L);
    assertThat(metrics.get(K2)).isEqualTo(2L);
    assertThat(metrics.get(K3)).isEqualTo(7L);
    assertThat(metrics.get(DURATION_KEY)).isAtLeast(0L);
    assertThrows(UnsupportedOperationException.class, () -> metrics.put(K1, 1L));
    assertThrows(UnsupportedOperationException.class, metrics::clear);
    assertThrows(UnsupportedOperationException.class, () -> firstEntry.setValue(99L));
  }

  @Test
  void measure_durationMetricAlsoRecordedBySupplier_overwritesInAllSlots() throws Exception {
    List<List<Metric>> cases =
        List.of(List.of(DURATION), List.of(M1, DURATION), List.of(M1, M2, DURATION));

    for (List<Metric> recorded : cases) {
      Map<MetricKey, Long> metrics =
          measureMetrics(READ_DURATION_OP, r -> recorded.forEach(m -> rec(r, m, Long.MAX_VALUE)));
      assertThat(metrics.get(DURATION_KEY)).isAtLeast(0L);
      assertThat(metrics.get(DURATION_KEY)).isLessThan(Long.MAX_VALUE);
    }
  }

  @Test
  void measure_supplierThrows_propagatesExceptionAndDeliversCollectedMetrics() {
    IllegalStateException boom = new IllegalStateException("boom");
    OperationSupplier<Object, RuntimeException> failing =
        r -> {
          rec(r, M1, 42L);
          throw boom;
        };

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> telemetry.measure(READ_DURATION_OP, failing));

    Map<MetricKey, Long> metrics = listener.getEndedMetrics().get(0);
    assertThat(thrown).isSameInstanceAs(boom);
    assertThat(metrics.get(K1)).isEqualTo(42L);
    assertThat(metrics.get(DURATION_KEY)).isAtLeast(0L);
  }

  @Test
  void measure_recorderUsedAfterMeasureReturns_doesNotMutateDeliveredMetrics() throws Exception {
    AtomicReference<MetricsRecorder> leaked = new AtomicReference<>();

    Map<MetricKey, Long> metrics =
        measureMetrics(
            READ_OP,
            r -> {
              rec(r, M1, 10L);
              leaked.set(r);
            });
    rec(leaked.get(), M1, 999L);

    assertThat(metrics).containsExactly(K1, 10L);
  }

  @Test
  void measure_concurrentRecordingAcrossThreads_accumulatesWithoutLostUpdates() throws Exception {
    int threads = 4;
    int iterations = 200;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CyclicBarrier barrier = new CyclicBarrier(threads);
    try {
      telemetry.measure(
          READ_OP,
          r -> {
            List<Future<Void>> futures =
                executor.invokeAll(
                    Collections.nCopies(
                        threads,
                        () -> {
                          barrier.await(5, TimeUnit.SECONDS);
                          for (int i = 0; i < iterations; i++) {
                            rec(r, M1, 1L);
                            rec(r, M2, 2L);
                          }
                          return null;
                        }));
            for (Future<Void> f : futures) {
              f.get();
            }
            return null;
          });
      Map<MetricKey, Long> metrics = listener.getEndedMetrics().get(0);
      assertThat(metrics.get(K1)).isEqualTo((long) threads * iterations);
      assertThat(metrics.get(K2)).isEqualTo(2L * threads * iterations);
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private Map<MetricKey, Long> measureMetrics(Operation op, Consumer<MetricsRecorder> body)
      throws Exception {
    telemetry.measure(
        op,
        r -> {
          body.accept(r);
          return null;
        });
    return listener.getEndedMetrics().get(listener.getEndedMetrics().size() - 1);
  }

  private static void rec(MetricsRecorder r, Metric m, long v) {
    r.record(m, v, Collections.emptyMap());
  }

  private static MetricKey keyOf(Metric metric) {
    return MetricKey.builder().setMetric(metric).build();
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
