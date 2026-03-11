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

import com.google.auto.value.AutoValue;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Options for OpenTelemetry integration. */
@AutoValue
public abstract class OpenTelemetryOptions {
  private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryOptions.class);

  /** Types of OpenTelemetry Metric Providers that can be specified by the user. */
  public enum ProviderType {
    /** Uses the global static OpenTelemetry instance via GlobalOpenTelemetry.get(). */
    GLOBAL,

    /** Uses a pre-configured OpenTelemetry instance explicitly provided to the builder. */
    PRE_CONFIGURED,

    /** Creates a periodic metric reader logging to standard output. */
    LOGGING
  }

  public abstract boolean isEnabled();

  public abstract ProviderType getProviderType();

  public abstract Optional<OpenTelemetry> getPreconfiguredOpenTelemetryInstance();

  public abstract int getExportIntervalSeconds();

  private static final String OPENTELEMETRY_ENABLED_KEY = "telemetry.opentelemetry.enabled";
  private static final String OPENTELEMETRY_PROVIDER_TYPE_KEY =
      "telemetry.opentelemetry.provider-type";
  private static final String OPENTELEMETRY_EXPORT_INTERVAL_SECONDS_KEY =
      "telemetry.opentelemetry.export-interval-seconds";

  public static Builder builder() {
    return new AutoValue_OpenTelemetryOptions.Builder()
        .setEnabled(false)
        .setExportIntervalSeconds(60)
        .setProviderType(ProviderType.GLOBAL);
  }

  public static Optional<OpenTelemetryOptions> createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    String enabled = analyticsCoreOptions.get(prefix + OPENTELEMETRY_ENABLED_KEY);
    String provider = analyticsCoreOptions.get(prefix + OPENTELEMETRY_PROVIDER_TYPE_KEY);
    String exportIntervalSeconds =
        analyticsCoreOptions.get(prefix + OPENTELEMETRY_EXPORT_INTERVAL_SECONDS_KEY);

    if (enabled == null && provider == null && exportIntervalSeconds == null) {
      return Optional.empty();
    }

    Builder builder = builder();
    if (enabled != null) {
      builder.setEnabled(Boolean.parseBoolean(enabled));
    }
    if (provider != null) {
      try {
        builder.setProviderType(ProviderType.valueOf(provider.toUpperCase()));
      } catch (IllegalArgumentException e) {
        LOG.warn("Invalid provider type provided: {}. Using default.", provider);
      }
    }
    if (exportIntervalSeconds != null) {
      try {
        builder.setExportIntervalSeconds(Integer.parseInt(exportIntervalSeconds));
      } catch (NumberFormatException e) {
        LOG.warn("Invalid export interval provided: '{}'. Using default.", exportIntervalSeconds);
      }
    }
    return Optional.of(builder.build());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setEnabled(boolean enabled);

    public abstract Builder setProviderType(ProviderType providerType);

    public abstract Builder setPreconfiguredOpenTelemetryInstance(OpenTelemetry openTelemetry);

    public abstract Builder setExportIntervalSeconds(int exportIntervalSeconds);

    public abstract OpenTelemetryOptions build();
  }
}
