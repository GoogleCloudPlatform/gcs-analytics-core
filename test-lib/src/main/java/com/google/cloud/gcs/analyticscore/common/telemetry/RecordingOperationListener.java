/*
 * Copyright 2026 Google LLC
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** An {@link OperationListener} that keeps emitted metric values in memory. */
public final class RecordingOperationListener implements OperationListener {

  private final Map<Metric, List<Long>> valuesByMetric = new ConcurrentHashMap<>();

  /** Returns the sum of the values emitted for {@code metric}, or zero if it was never emitted. */
  public long getTotal(Metric metric) {
    long total = 0;
    for (long value : getValues(metric)) {
      total += value;
    }
    return total;
  }

  /** Returns every value emitted for {@code metric}, in the order it was emitted. */
  public List<Long> getValues(Metric metric) {
    return List.copyOf(valuesByMetric.getOrDefault(metric, List.of()));
  }

  @Override
  public void onOperationStart(Operation operation) {}

  @Override
  public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
    metrics.forEach(
        (key, value) ->
            valuesByMetric
                .computeIfAbsent(key.getMetric(), metric -> new CopyOnWriteArrayList<>())
                .add(value));
  }
}
