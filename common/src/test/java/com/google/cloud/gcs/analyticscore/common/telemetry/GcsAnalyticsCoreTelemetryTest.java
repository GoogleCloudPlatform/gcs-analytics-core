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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GcsAnalyticsCoreTelemetryTest {

  private GcsAnalyticsCoreTelemetry telemetry;
  private FakeGcsOperationMetricsListener listener;

  @BeforeEach
  void setUp() {
    telemetry = GcsAnalyticsCoreTelemetry.getInstance();
    listener = new FakeGcsOperationMetricsListener();
    telemetry.addListener(listener);
  }

  @AfterEach
  void tearDown() {
    telemetry.removeListener(listener);
  }

  @Test
  void measure_validOperation_returnsResultAndRecordsMetrics() throws Exception {
    GcsOperation operation =
        GcsOperation.builder()
            .setType(GcsOperationType.READ)
            .setDurationMetricName(Optional.of("duration"))
            .build();

    String result = telemetry.measure(operation, recorder -> "result");
    Map<MetricKey, Long> metrics = listener.getEndedMetrics().get(0);

    assertEquals("result", result);
    assertEquals(1, listener.getStartedOperations().size());
    assertEquals(operation, listener.getStartedOperations().get(0));
    assertEquals(1, listener.getEndedOperations().size());
    assertEquals(operation, listener.getEndedOperations().get(0));
    assertEquals(1, listener.getEndedMetrics().size());
    assertTrue(metrics.keySet().stream().anyMatch(key -> "duration".equals(key.getName())));
  }

  @Test
  void recordMetric_validInput_recordsMetricWithTypeNA() {
    String name = "testMetric";
    long value = 123L;
    TelemetryAttributes attributes = TelemetryAttributes.builder().build();

    telemetry.recordMetric(name, value, attributes);
    Map<MetricKey, Long> metrics = listener.getEndedMetrics().get(0);
    MetricKey key = metrics.keySet().iterator().next();

    assertEquals(1, listener.getEndedOperations().size());
    assertEquals(GcsOperationType.NA, listener.getEndedOperations().get(0).getType());
    assertEquals(1, metrics.size());
    assertEquals(name, key.getName());
    assertEquals(attributes, key.getAttributes());
    assertEquals(Long.valueOf(value), metrics.get(key));
  }
}
