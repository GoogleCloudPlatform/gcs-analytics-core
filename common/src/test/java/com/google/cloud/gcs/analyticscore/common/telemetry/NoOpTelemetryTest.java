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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class NoOpTelemetryTest {

  private final NoOpTelemetry telemetry = new NoOpTelemetry();

  @Test
  void measure_withOperation_returnsSupplierResult() throws Exception {
    Operation operation = Operation.builder().setName("READ").build();

    String result = telemetry.measure(operation, recorder -> "result");

    assertThat(result).isEqualTo("result");
  }

  @Test
  void measure_withOperationName_returnsSupplierResult() throws Exception {
    String result =
        telemetry.measure(
            "READ",
            TestMetric.of("duration", Metric.MetricType.DURATION),
            Collections.emptyMap(),
            recorder -> "result");

    assertThat(result).isEqualTo("result");
  }

  @Test
  void measure_withOperationId_returnsSupplierResult() throws Exception {
    String result =
        telemetry.measure(
            "operation-id",
            "READ",
            TestMetric.of("duration", Metric.MetricType.DURATION),
            Collections.emptyMap(),
            recorder -> "result");

    assertThat(result).isEqualTo("result");
  }

  @Test
  void measure_supplierRecordsMetrics_isAcceptedAndDiscarded() throws Exception {
    Metric counter = TestMetric.of("bytes", Metric.MetricType.COUNTER);

    String result =
        telemetry.measure(
            "READ",
            TestMetric.of("duration", Metric.MetricType.DURATION),
            Collections.emptyMap(),
            recorder -> {
              // Suppliers must not have to know which implementation they were handed.
              recorder.record(counter, 42L, Collections.emptyMap());
              return "result";
            });

    assertThat(result).isEqualTo("result");
  }

  @Test
  void measure_propagatesSupplierException() {
    IllegalStateException thrown = new IllegalStateException("boom");

    IllegalStateException actual =
        assertThrows(
            IllegalStateException.class,
            () ->
                telemetry.measure(
                    Operation.builder().setName("READ").build(),
                    recorder -> {
                      throw thrown;
                    }));

    assertThat(actual).isSameInstanceAs(thrown);
  }

  @Test
  void recordMetric_doesNotThrow() {
    assertDoesNotThrow(
        () ->
            telemetry.recordMetric(
                TestMetric.of("bytes", Metric.MetricType.COUNTER), 1L, Collections.emptyMap()));
  }

  @Test
  void measure_afterClose_stillReturnsSupplierResult() throws Exception {
    telemetry.close();

    String result =
        telemetry.measure(Operation.builder().setName("READ").build(), recorder -> "result");

    assertThat(result).isEqualTo("result");
  }

  /**
   * Inheriting a method from {@link Telemetry} means inheriting its work, which is exactly what
   * this class exists to avoid. A method added to {@link Telemetry} and not overridden here would
   * otherwise start doing real work on the no-op path, silently and without failing anything.
   */
  @Test
  void noOpTelemetry_overridesEveryPublicMethodOfTelemetry() {
    List<String> notOverridden = new ArrayList<>();
    for (Method method : Telemetry.class.getDeclaredMethods()) {
      if (!Modifier.isPublic(method.getModifiers()) || Modifier.isStatic(method.getModifiers())) {
        continue;
      }
      try {
        var unused =
            NoOpTelemetry.class.getDeclaredMethod(method.getName(), method.getParameterTypes());
      } catch (NoSuchMethodException e) {
        notOverridden.add(method.getName() + Arrays.toString(method.getParameterTypes()));
      }
    }

    assertThat(notOverridden).isEmpty();
  }
}
