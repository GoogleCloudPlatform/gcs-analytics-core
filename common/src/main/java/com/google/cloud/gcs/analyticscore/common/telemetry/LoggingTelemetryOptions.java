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

import com.google.auto.value.AutoValue;
import com.google.cloud.gcs.analyticscore.common.ConfigurationUtil;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Options for Logging Telemetry. */
@AutoValue
public abstract class LoggingTelemetryOptions {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingTelemetryOptions.class);

  public enum LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARNING,
    ERROR
  }

  public abstract LogLevel getLogLevel();

  public abstract boolean isEnabled();

  private static final String LOGGING_TELEMETRY_ENABLED_KEY = "telemetry.logging.enabled";
  private static final String LOGGING_TELEMETRY_LEVEL_KEY = "telemetry.logging.level";

  public static Builder builder() {
    return new AutoValue_LoggingTelemetryOptions.Builder()
        .setEnabled(false)
        .setLogLevel(LogLevel.DEBUG);
  }

  public static Optional<LoggingTelemetryOptions> createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    Optional<String> enabled =
        ConfigurationUtil.getTrimmedValue(
            analyticsCoreOptions, prefix + LOGGING_TELEMETRY_ENABLED_KEY);
    Optional<String> level =
        ConfigurationUtil.getTrimmedValue(
            analyticsCoreOptions, prefix + LOGGING_TELEMETRY_LEVEL_KEY);

    if (enabled.isEmpty() && level.isEmpty()) {
      return Optional.empty();
    }

    Builder builder = builder();
    if (enabled.isPresent()) {
      builder.setEnabled(Boolean.parseBoolean(enabled.get()));
    }
    if (level.isPresent()) {
      String levelValue = level.get();
      try {
        builder.setLogLevel(LogLevel.valueOf(levelValue.toUpperCase(Locale.ROOT)));
      } catch (IllegalArgumentException e) {
        LOG.warn("Invalid log level provided: {}. Using default.", levelValue);
      }
    }
    return Optional.of(builder.build());
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setLogLevel(LogLevel logLevel);

    public abstract Builder setEnabled(boolean enabled);

    public abstract LoggingTelemetryOptions build();
  }
}
