/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.gcs.analyticscore.common.telemetry;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongHistogramBuilder;
import io.opentelemetry.api.metrics.Meter;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class OpenTelemetryReporterTest {

  private OpenTelemetry mockOpenTelemetry;
  private Meter mockMeter;

  private DoubleHistogramBuilder mockDoubleHistogramBuilder;
  private LongHistogramBuilder mockHistogramBuilder;
  private LongHistogram mockHistogram;

  private LongCounterBuilder mockCounterBuilder;
  private LongCounter mockCounter;

  @BeforeEach
  void setUp() {
    mockOpenTelemetry = mock(OpenTelemetry.class);
    mockMeter = mock(Meter.class);
    when(mockOpenTelemetry.getMeter(any(String.class))).thenReturn(mockMeter);
    mockDoubleHistogramBuilder = mock(DoubleHistogramBuilder.class);
    mockHistogramBuilder = mock(LongHistogramBuilder.class);
    mockHistogram = mock(LongHistogram.class);
    when(mockMeter.histogramBuilder(any(String.class))).thenReturn(mockDoubleHistogramBuilder);
    when(mockDoubleHistogramBuilder.ofLongs()).thenReturn(mockHistogramBuilder);
    when(mockHistogramBuilder.build()).thenReturn(mockHistogram);
    mockCounterBuilder = mock(LongCounterBuilder.class);
    mockCounter = mock(LongCounter.class);
    when(mockMeter.counterBuilder(any(String.class))).thenReturn(mockCounterBuilder);
    when(mockCounterBuilder.build()).thenReturn(mockCounter);
  }

  @Test
  void testOperationEnd_recordsMetrics() {
    OpenTelemetryOptions options =
        OpenTelemetryOptions.builder()
            .setEnabled(true)
            .setProviderType(OpenTelemetryOptions.ProviderType.PRE_CONFIGURED)
            .setPreconfiguredOpenTelemetryInstance(mockOpenTelemetry)
            .build();
    try (OpenTelemetryReporter reporter = new OpenTelemetryReporter(options)) {
      Map<String, String> opAttrs = new HashMap<>();
      opAttrs.put("opId", "123");
      Operation operation =
          Operation.builder()
              .setName("testOp")
              .setDurationMetricName("testOp.duration")
              .setAttributes(opAttrs)
              .build();
      Map<MetricKey, Long> metrics = new HashMap<>();
      metrics.put(
          MetricKey.builder()
              .setName("testOp.duration")
              .setMetricType(MetricKey.MetricType.DURATION)
              .build(),
          1500L);
      Map<String, String> metricAttrs = new HashMap<>();
      metricAttrs.put("status", "OK");
      metrics.put(
          MetricKey.builder().setName("testOp.bytes").setAttributes(metricAttrs).build(), 1024L);

      reporter.onOperationEnd(operation, metrics);

      ArgumentCaptor<Attributes> histogramAttrsCaptor = ArgumentCaptor.forClass(Attributes.class);
      verify(mockHistogram).record(eq(1500L), histogramAttrsCaptor.capture());
      assertThat(histogramAttrsCaptor.getValue().get(AttributeKey.stringKey("opId")))
          .isEqualTo("123");
      ArgumentCaptor<Attributes> counterAttrsCaptor = ArgumentCaptor.forClass(Attributes.class);
      verify(mockCounter).add(eq(1024L), counterAttrsCaptor.capture());
      Attributes counterAttributes = counterAttrsCaptor.getValue();
      assertThat(counterAttributes.get(AttributeKey.stringKey("opId"))).isEqualTo("123");
      assertThat(counterAttributes.get(AttributeKey.stringKey("status"))).isEqualTo("OK");
    }
  }
}
