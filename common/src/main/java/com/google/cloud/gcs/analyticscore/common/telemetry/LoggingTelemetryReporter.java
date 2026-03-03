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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A telemetry reporter that logs operations and their metrics using SLF4J. The format and log level
 * are customizable through {@link LoggingTelemetryOptions}.
 */
public class LoggingTelemetryReporter implements OperationListener {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingTelemetryReporter.class);

  private final LoggingTelemetryOptions options;

  public LoggingTelemetryReporter(LoggingTelemetryOptions options) {
    this.options = options;
  }

  @Override
  public void onOperationStart(Operation operation) {
    String message =
        String.format(
            "Operation started: [%s], id: [%s], attributes: %s",
            operation.getName(), operation.getOperationId(), operation.getAttributes());
    logMessage(message);
  }

  @Override
  public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
    String message =
        String.format(
            "Operation ended: [%s], id: [%s], attributes: %s, metrics: %s",
            operation.getName(),
            operation.getOperationId(),
            operation.getAttributes(),
            formatMetrics(metrics));
    logMessage(message);
  }

  /**
   * Formats a map of metrics into a generic, readable string. Sample : {metric1=1, metric2=2,
   * metric3=3}
   */
  @VisibleForTesting
  String formatMetrics(Map<MetricKey, Long> metrics) {
    if (metrics == null || metrics.isEmpty()) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<MetricKey, Long> entry : metrics.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      MetricKey key = entry.getKey();
      sb.append(key.getMetric().getName());
      if (key.getAttributes() != null && !key.getAttributes().isEmpty()) {
        sb.append(key.getAttributes());
      }
      sb.append("=").append(entry.getValue());
    }
    sb.append("}");
    return sb.toString();
  }

  private void logMessage(String message) {
    switch (options.getLogLevel()) {
      case TRACE:
        LOG.trace(message);
        break;
      case DEBUG:
        LOG.debug(message);
        break;
      case WARNING:
        LOG.warn(message);
        break;
      case ERROR:
        LOG.error(message);
        break;
      case INFO:
      default:
        LOG.info(message);
        break;
    }
  }
}
