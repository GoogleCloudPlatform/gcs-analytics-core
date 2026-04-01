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
import java.util.Map;
import java.util.Optional;

/** Options for Telemetry. */
@AutoValue
public abstract class TelemetryOptions {

  public abstract Optional<CustomTelemetryOptions> getCustomTelemetryOptions();

  public abstract Optional<LoggingTelemetryOptions> getLoggingTelemetryOptions();

  public abstract Optional<OpenTelemetryOptions> getOpenTelemetryOptions();

  public static Builder builder() {
    return new AutoValue_TelemetryOptions.Builder();
  }

  public static TelemetryOptions createFromOptions(
      Map<String, String> analyticsCoreOptions, String prefix) {
    TelemetryOptions.Builder optionsBuilder = builder();

    LoggingTelemetryOptions.createFromOptions(analyticsCoreOptions, prefix)
        .ifPresent(optionsBuilder::setLoggingTelemetryOptions);

    OpenTelemetryOptions.createFromOptions(analyticsCoreOptions, prefix)
        .ifPresent(optionsBuilder::setOpenTelemetryOptions);

    return optionsBuilder.build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setCustomTelemetryOptions(CustomTelemetryOptions options);

    public abstract Builder setLoggingTelemetryOptions(LoggingTelemetryOptions options);

    public abstract Builder setOpenTelemetryOptions(OpenTelemetryOptions options);

    public abstract TelemetryOptions build();
  }
}
