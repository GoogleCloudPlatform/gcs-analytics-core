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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultTelemetryFormatterTest {

  private DefaultTelemetryFormatter formatter;

  @BeforeEach
  void setUp() {
    formatter = new DefaultTelemetryFormatter();
  }

  @Test
  void formatOperationStart_returnsFormattedString() {
    Operation operation = Operation.builder().setName("READ").build();

    String result = formatter.formatOperationStart(operation);

    assertEquals("Operation READ started", result);
  }

  @Test
  void formatOperationEnd_singleMetric_returnsFormattedString() {
    Operation operation = Operation.builder().setName("READ").build();
    MetricKey key = MetricKey.builder().setName("bytes").build();

    String result = formatter.formatOperationEnd(operation, ImmutableMap.of(key, 100L));

    assertEquals("Operation READ: bytes=100", result);
  }

  @Test
  void formatOperationEnd_withAttributesAndMultipleMetrics_returnsFormattedString() {
    Operation operation = Operation.builder().setName("READ").build();
    Map<String, String> attributes =
        ImmutableMap.of("className", "TestClass", "readLength", "1024");
    MetricKey key1 = MetricKey.builder().setName("bytes").setAttributes(attributes).build();
    MetricKey key2 = MetricKey.builder().setName("latency").build();
    Map<MetricKey, Long> metrics =
        com.google.common.collect.ImmutableMap.of(key1, 1024L, key2, 123L);

    String result = formatter.formatOperationEnd(operation, metrics);

    assertTrue(result.startsWith("Operation READ: "));
    assertTrue(result.contains("bytes{className=TestClass, readLength=1024}=1024"));
    assertTrue(result.contains("latency=123"));
  }
}
