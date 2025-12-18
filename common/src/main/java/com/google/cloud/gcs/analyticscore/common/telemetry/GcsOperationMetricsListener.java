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

import java.util.Map;

public interface GcsOperationMetricsListener {
  /**
   * Triggered when an operation starts.
   *
   * @param operation the operation context
   */
  void onOperationStart(GcsOperation operation);

  /**
   * Triggered when an operation ends.
   *
   * @param operation the operation context
   * @param metrics a map of collected metrics (key includes name and attributes)
   */
  void onOperationEnd(GcsOperation operation, Map<MetricKey, Long> metrics);
}
