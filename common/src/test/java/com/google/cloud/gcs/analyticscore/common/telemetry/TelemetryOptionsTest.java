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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TelemetryOptionsTest {

  @Test
  public void testBuilderWithCustomTelemetryOptions() {
    OperationListener listener =
        new OperationListener() {
          @Override
          public void onOperationStart(Operation operation) {}

          @Override
          public void onOperationEnd(Operation operation, java.util.Map<MetricKey, Long> metrics) {}
        };
    CustomTelemetryOptions customTelemetryOptions =
        CustomTelemetryOptions.builder().setOperationListeners(ImmutableList.of(listener)).build();
    TelemetryOptions options =
        TelemetryOptions.builder().setCustomTelemetryOptions(customTelemetryOptions).build();

    assertThat(options.getCustomTelemetryOptions().get().getOperationListeners())
        .containsExactly(listener);
  }
}
