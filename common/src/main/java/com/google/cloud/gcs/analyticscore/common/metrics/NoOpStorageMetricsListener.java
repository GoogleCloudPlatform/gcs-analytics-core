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

package com.google.cloud.gcs.analyticscore.common.metrics;

/* A no-operation implementation of {@link MetricsListener}.
 * Provides empty implementations for all listener methods, allowing subclasses to override only the methods they are interested in.
 */
public abstract class NoOpStorageMetricsListener implements StorageMetricsListener {

  /** Singleton instance of NoOpMetricsListener. */
  public static final StorageMetricsListener INSTANCE = new NoOpStorageMetricsListener() {};

  @Override
  public void onOperationLatency(StorageOperationType operationType, long durationMs) {}

  @Override
  public void onBytesProcessed(StorageOperationType operationType, long bytes) {}

  @Override
  public void onOperation(StorageOperationType operationType) {}
}
