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
import org.slf4j.event.Level;

/**
 * A telemetry reporter that logs operations and their metrics using SLF4J. The format and log level
 * are customizable through {@link LoggingTelemetryOptions}.
 */
public class LoggingTelemetryReporter implements OperationListener {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingTelemetryReporter.class);

  private final Logger log;
  private final Level level;

  public LoggingTelemetryReporter(LoggingTelemetryOptions options) {
    this(options, LOG);
  }

  /**
   * Exists so that tests can observe what is logged, and at which level, without depending on the
   * logging backend that happens to be on the test classpath.
   */
  @VisibleForTesting
  LoggingTelemetryReporter(LoggingTelemetryOptions options, Logger log) {
    this.log = log;
    this.level = toSlf4jLevel(options.getLogLevel());
  }

  @Override
  public void onOperationStart(Operation operation) {
    log.atLevel(level)
        .setMessage("Operation started: [{}], id: [{}], attributes: {}")
        .addArgument(operation.getName())
        .addArgument(operation.getOperationId())
        .addArgument(operation.getAttributes())
        .log();
  }

  @Override
  public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
    // The fluent API checks the level itself, but only after the argument list has been built, so
    // the supplier below would be allocated even when nothing is listening: measured at 24 B/op on
    // an operation that is otherwise free. Hence the explicit check. Unlike the level check this
    // replaced, it enumerates nothing, so no second switch can fall out of sync with the mapping.
    if (!log.isEnabledForLevel(level)) {
      return;
    }
    // Rendering the metric map is the expensive part of reporting, so it is passed as a supplier:
    // SLF4J only resolves it once the level is known to be enabled. The remaining arguments are
    // existing objects whose toString() is likewise deferred until the message is formatted.
    log.atLevel(level)
        .setMessage("Operation ended: [{}], id: [{}], attributes: {}, metrics: {}")
        .addArgument(operation.getName())
        .addArgument(operation.getOperationId())
        .addArgument(operation.getAttributes())
        .addArgument(() -> formatMetrics(metrics))
        .log();
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
      if (!key.getAttributes().isEmpty()) {
        sb.append(key.getAttributes());
      }
      sb.append("=").append(entry.getValue());
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Translates the configured level into its SLF4J equivalent once, at construction.
   *
   * <p>This is the only place the two enums are related to each other. Deciding "is this level
   * enabled" and "which method emits at this level" are both left to SLF4J, so a level added to
   * {@link LoggingTelemetryOptions.LogLevel} cannot end up handled by one and missed by the other.
   */
  private static Level toSlf4jLevel(LoggingTelemetryOptions.LogLevel logLevel) {
    switch (logLevel) {
      case TRACE:
        return Level.TRACE;
      case DEBUG:
        return Level.DEBUG;
      case WARNING:
        return Level.WARN;
      case ERROR:
        return Level.ERROR;
      case INFO:
      default:
        return Level.INFO;
    }
  }
}
