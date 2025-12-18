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
import java.util.Optional;
import java.util.UUID;

@AutoValue
public abstract class GcsOperation {

  public abstract GcsOperationType getType();

  public abstract TelemetryAttributes getAttributes();

  public abstract String getOperationId();

  public abstract Optional<String> getDurationMetricName();

  public static Builder builder() {
    return new AutoValue_GcsOperation.Builder()
        .setAttributes(TelemetryAttributes.builder().build())
        .setOperationId(UUID.randomUUID().toString());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setType(GcsOperationType type);

    public abstract Builder setAttributes(TelemetryAttributes attributes);

    public abstract Builder setOperationId(String operationId);

    public abstract Builder setDurationMetricName(String durationMetricName);

    public abstract Builder setDurationMetricName(Optional<String> durationMetricName);

    public abstract GcsOperation build();
  }
}
