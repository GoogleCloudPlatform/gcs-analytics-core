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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class LogTelemetryReporterTest {

  @Mock private Logger mockLogger;
  @Mock private TelemetryFormatter mockFormatter;

  private GcsOperation operation;
  private Map<MetricKey, Long> metrics;

  @BeforeEach
  void setUp() {
    metrics = Collections.emptyMap();
    operation =
        GcsOperation.builder()
            .setType(GcsOperationType.VECTORED_READ)
            .setOperationId("test-op-id")
            .build();
  }

  @Test
  void onOperationStart_logsAtDebug_whenLevelIsDebug() {
    when(mockLogger.isDebugEnabled()).thenReturn(true);
    when(mockFormatter.formatOperationStart(operation)).thenReturn("start message");

    LogTelemetryReporter reporter = new LogTelemetryReporter(mockLogger, mockFormatter, "DEBUG");
    reporter.onOperationStart(operation);

    verify(mockLogger).debug("start message");
  }

  @Test
  void onOperationEnd_logsAtInfo_whenLevelIsInfo() {
    when(mockLogger.isInfoEnabled()).thenReturn(true);
    when(mockFormatter.formatOperationEnd(operation, metrics)).thenReturn("end message");

    LogTelemetryReporter reporter = new LogTelemetryReporter(mockLogger, mockFormatter, "INFO");
    reporter.onOperationEnd(operation, metrics);

    verify(mockLogger).info("end message");
  }

  @Test
  void onOperationStart_doesNotLog_whenLevelDisabled() {
    when(mockLogger.isTraceEnabled()).thenReturn(false);
    when(mockFormatter.formatOperationStart(operation)).thenReturn("start message");

    LogTelemetryReporter reporter = new LogTelemetryReporter(mockLogger, mockFormatter, "TRACE");
    reporter.onOperationStart(operation);

    verify(mockLogger).isTraceEnabled();
    verify(mockLogger, never()).trace(anyString());
  }

  @Test
  void onOperationStart_logsAtError() {
    when(mockLogger.isErrorEnabled()).thenReturn(true);
    when(mockFormatter.formatOperationStart(operation)).thenReturn("error msg");

    LogTelemetryReporter reporter = new LogTelemetryReporter(mockLogger, mockFormatter, "ERROR");
    reporter.onOperationStart(operation);

    verify(mockLogger).error("error msg");
  }

  @Test
  void onOperationStart_logsAtWarn() {
    when(mockLogger.isWarnEnabled()).thenReturn(true);
    when(mockFormatter.formatOperationStart(operation)).thenReturn("warn msg");

    LogTelemetryReporter reporter = new LogTelemetryReporter(mockLogger, mockFormatter, "WARN");
    reporter.onOperationStart(operation);

    verify(mockLogger).warn("warn msg");
  }

  @Test
  void constructor_defaultsToDebug_whenLevelIsNull() {
    when(mockLogger.isDebugEnabled()).thenReturn(true);
    when(mockFormatter.formatOperationStart(operation)).thenReturn("default msg");

    LogTelemetryReporter reporter = new LogTelemetryReporter(mockLogger, mockFormatter, null);
    reporter.onOperationStart(operation);

    verify(mockLogger).debug("default msg");
  }

  @Test
  void constructor_throwsException_whenLevelIsInvalid() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LogTelemetryReporter(mockLogger, mockFormatter, "INVALID_LEVEL"));
  }
}
