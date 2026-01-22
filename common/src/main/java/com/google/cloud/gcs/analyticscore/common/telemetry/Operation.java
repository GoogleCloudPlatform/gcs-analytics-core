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
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@AutoValue
public abstract class Operation {

  public abstract String getName();

  public abstract Map<String, String> getAttributes();

  public abstract String getOperationId();

  public abstract Optional<String> getDurationMetricName();

  public static Builder builder() {
    return new AutoValue_Operation.Builder()
        .setAttributes(Collections.emptyMap())
        .setOperationId(UUID.randomUUID().toString());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setName(String name);

    public abstract Builder setAttributes(Map<String, String> attributes);

    public abstract Builder setOperationId(String operationId);

    public abstract Builder setDurationMetricName(String durationMetricName);

    public abstract Operation build();
  }
}
