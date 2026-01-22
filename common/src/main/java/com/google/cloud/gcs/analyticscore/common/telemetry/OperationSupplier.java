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

@FunctionalInterface
public interface OperationSupplier<T, E extends Throwable> {
  /**
   * Supplies the operation logic, potentially throwing an exception.
   *
   * @param recorder The recorder to capture arbitrary metrics during execution
   * @return the result of the operation
   * @throws E if the operation fails
   */
  T get(MetricsRecorder recorder) throws E;
}
