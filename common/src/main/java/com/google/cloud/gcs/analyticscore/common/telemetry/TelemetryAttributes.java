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

/** Represents a set of attributes for telemetry context. */
@AutoValue
public abstract class TelemetryAttributes {

  public abstract Optional<String> className();

  public abstract Optional<Long> readOffset();

  public abstract Optional<Long> readLength();

  public abstract Optional<String> threadId();

  public static Builder builder() {
    return new AutoValue_TelemetryAttributes.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setClassName(String className);

    public abstract Builder setReadOffset(Long readOffset);

    public abstract Builder setReadLength(Long readLength);

    public abstract Builder setThreadId(String threadId);

    public abstract TelemetryAttributes build();
  }
}
