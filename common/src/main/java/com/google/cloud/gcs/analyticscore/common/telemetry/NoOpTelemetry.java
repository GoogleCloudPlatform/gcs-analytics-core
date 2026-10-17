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

import java.util.Collections;
import java.util.Map;

/**
 * A {@link Telemetry} that collects nothing, for when no listener is configured. Package-private:
 * instances are obtained from {@link Telemetry#create}.
 *
 * <p>{@code measure} wraps every {@code read}, {@code seek} and {@code write}, so the question "is
 * anyone listening?" would otherwise be asked on every data-plane operation. Answering it once, by
 * choosing an implementation at construction, keeps that question off the hot path entirely.
 *
 * <p>Suppliers are still invoked, and still receive a usable {@link MetricsRecorder}; it simply
 * discards what it is given. Callers cannot tell the difference apart from the absence of reported
 * metrics.
 *
 * <p>Every public method of {@link Telemetry} is overridden here, including {@link #close()}, which
 * would be harmless to inherit. That is deliberate: inheriting a method means inheriting its work,
 * and a no-op that quietly does work is the bug this class exists to prevent. {@code
 * NoOpTelemetryTest} fails if a method is ever added to {@link Telemetry} without being overridden
 * here.
 */
final class NoOpTelemetry extends Telemetry {

  /** Discards every metric recorded against it. */
  private static final MetricsRecorder NO_OP_RECORDER = (metric, value, attributes) -> {};

  NoOpTelemetry() {
    super(Collections.emptyList());
  }

  @Override
  public <T, E extends Throwable> T measure(
      Operation operation, OperationSupplier<T, E> operationSupplier) throws E {
    return operationSupplier.get(NO_OP_RECORDER);
  }

  @Override
  public <T, E extends Throwable> T measure(
      String operationId,
      String operationName,
      Metric durationMetric,
      Map<String, String> operationAttributes,
      OperationSupplier<T, E> operationSupplier)
      throws E {
    return operationSupplier.get(NO_OP_RECORDER);
  }

  @Override
  public <T, E extends Throwable> T measure(
      String operationName,
      Metric durationMetric,
      Map<String, String> operationAttributes,
      OperationSupplier<T, E> operationSupplier)
      throws E {
    return operationSupplier.get(NO_OP_RECORDER);
  }

  @Override
  public void recordMetric(Metric metric, long value, Map<String, String> attributes) {}

  @Override
  public void close() {}
}
