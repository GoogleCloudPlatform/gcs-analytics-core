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

import io.opentelemetry.api.OpenTelemetry;

/** Interface for providing and managing OpenTelemetry instances. */
public interface OpenTelemetryProvider extends AutoCloseable {

  /**
   * Returns the referenced OpenTelemetry instance.
   *
   * @return the OpenTelemetry instance.
   */
  OpenTelemetry getOpenTelemetry();

  /** Shuts down the provider, releasing any internal resources bound to it. */
  @Override
  default void close() {}
}
