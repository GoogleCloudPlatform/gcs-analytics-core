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
import com.google.common.collect.ImmutableList;
import java.util.List;

/** Options for Custom Telemetry. */
@AutoValue
public abstract class CustomTelemetryOptions {

  public abstract ImmutableList<OperationListener> getOperationListeners();

  public static Builder builder() {
    return new AutoValue_CustomTelemetryOptions.Builder().setOperationListeners(ImmutableList.of());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setOperationListeners(List<OperationListener> operationListeners);

    public abstract CustomTelemetryOptions build();
  }
}
