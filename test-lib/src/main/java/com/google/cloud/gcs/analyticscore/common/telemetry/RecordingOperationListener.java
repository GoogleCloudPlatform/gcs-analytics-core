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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An {@link OperationListener} that accumulates emitted metric values in memory.
 *
 * <p>Values for the same metric are summed across operations, matching how a real reporter would
 * aggregate a counter.
 */
public final class RecordingOperationListener implements OperationListener {

  private final Map<Metric, Long> totalsByMetric = new ConcurrentHashMap<>();

  /** Returns the summed value recorded for {@code metric}, or zero if it was never emitted. */
  public long getTotal(Metric metric) {
    return totalsByMetric.getOrDefault(metric, 0L);
  }

  @Override
  public void onOperationStart(Operation operation) {}

  @Override
  public void onOperationEnd(Operation operation, Map<MetricKey, Long> metrics) {
    metrics.forEach((key, value) -> totalsByMetric.merge(key.getMetric(), value, Long::sum));
  }
}
