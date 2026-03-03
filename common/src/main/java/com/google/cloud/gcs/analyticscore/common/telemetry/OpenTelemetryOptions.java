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
import java.util.Optional;

/** Options for OpenTelemetry integration. */
@AutoValue
public abstract class OpenTelemetryOptions {

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

  public static Builder builder() {
    return new AutoValue_OpenTelemetryOptions.Builder()
        .setEnabled(false)
        .setExportIntervalSeconds(60)
        .setProviderType(ProviderType.GLOBAL);
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
