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

import autovalue.shaded.com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/** A listener that logs telemetry events. */
public class LogTelemetryReporter implements GcsOperationMetricsListener {

  private final Logger logger;
  private final TelemetryFormatter formatter;
  private final Level level;

  public LogTelemetryReporter(TelemetryFormatter formatter, String level) {
    this(LoggerFactory.getLogger(LogTelemetryReporter.class.getName()), formatter, level);
  }

  @VisibleForTesting
  protected LogTelemetryReporter(Logger logger, TelemetryFormatter formatter, String level) {
    this.logger = logger;
    this.formatter = formatter != null ? formatter : new DefaultTelemetryFormatter();
    this.level = level != null ? Level.valueOf(level.toUpperCase()) : Level.DEBUG;
  }

  @Override
  public void onOperationStart(GcsOperation operation) {
    logAtLevel(formatter.formatOperationStart(operation));
  }

  @Override
  public void onOperationEnd(GcsOperation operation, Map<MetricKey, Long> metrics) {
    logAtLevel(formatter.formatOperationEnd(operation, metrics));
  }

  private void logAtLevel(String message) {
    switch (level) {
      case ERROR:
        if (logger.isErrorEnabled()) logger.error(message);
        break;
      case WARN:
        if (logger.isWarnEnabled()) logger.warn(message);
        break;
      case INFO:
        if (logger.isInfoEnabled()) logger.info(message);
        break;
      case DEBUG:
        if (logger.isDebugEnabled()) logger.debug(message);
        break;
      case TRACE:
        if (logger.isTraceEnabled()) logger.trace(message);
        break;
    }
  }
}
