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
    Operation operation =
        Operation.builder().setName("READ").setDurationMetricName("duration").build();

    String result =
        telemetry.measure(
            operation.getName(),
            operation.getDurationMetricName().orElse(null),
            operation.getAttributes(),
            recorder -> "result");

    Map<MetricKey, Long> metrics = listener.getEndedMetrics().get(0);
    Operation startedOp = listener.getStartedOperations().get(0);
    Operation endedOp = listener.getEndedOperations().get(0);
    assertThat(result).isEqualTo("result");
    assertThat(listener.getStartedOperations()).hasSize(1);
    assertThat(startedOp.getName()).isEqualTo(operation.getName());
    assertThat(startedOp.getAttributes()).isEqualTo(operation.getAttributes());
    assertThat(startedOp.getDurationMetricName()).isEqualTo(operation.getDurationMetricName());
    assertThat(listener.getEndedOperations()).hasSize(1);
    assertThat(endedOp.getName()).isEqualTo(operation.getName());
    assertThat(endedOp.getAttributes()).isEqualTo(operation.getAttributes());
    assertThat(endedOp.getDurationMetricName()).isEqualTo(operation.getDurationMetricName());
    assertThat(listener.getEndedMetrics()).hasSize(1);
    assertThat(metrics.keySet().stream().anyMatch(key -> "duration".equals(key.getName())))
        .isTrue();
  }

  @Test
  void recordMetric_validInput_recordsMetricWithTypeNA() {
    String name = "testMetric";
    long value = 123L;
    Map<String, String> attributes = Collections.emptyMap();

    telemetry.recordMetric(name, value, attributes);

    Map<MetricKey, Long> metrics = listener.getEndedMetrics().get(0);
    MetricKey key = metrics.keySet().iterator().next();
    assertThat(listener.getEndedOperations()).hasSize(1);
    assertThat(listener.getEndedOperations().get(0).getName()).isEqualTo("UNKNOWN");
    assertThat(metrics).hasSize(1);
    assertThat(key.getName()).isEqualTo(name);
    assertThat(key.getAttributes()).isEqualTo(attributes);
    assertThat(metrics.get(key)).isEqualTo(value);
  }
}
