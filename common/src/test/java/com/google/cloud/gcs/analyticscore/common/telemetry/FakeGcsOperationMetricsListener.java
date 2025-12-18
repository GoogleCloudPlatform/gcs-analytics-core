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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FakeGcsOperationMetricsListener implements GcsOperationMetricsListener {

  private final List<GcsOperation> startedOperations = new ArrayList<>();
  private final List<GcsOperation> endedOperations = new ArrayList<>();
  private final List<Map<MetricKey, Long>> endedMetrics = new ArrayList<>();

  @Override
  public void onOperationStart(GcsOperation operation) {
    startedOperations.add(operation);
  }

  @Override
  public void onOperationEnd(GcsOperation operation, Map<MetricKey, Long> metrics) {
    endedOperations.add(operation);
    endedMetrics.add(metrics);
  }

  public List<GcsOperation> getStartedOperations() {
    return startedOperations;
  }

  public List<GcsOperation> getEndedOperations() {
    return endedOperations;
  }

  public List<Map<MetricKey, Long>> getEndedMetrics() {
    return endedMetrics;
  }
}
