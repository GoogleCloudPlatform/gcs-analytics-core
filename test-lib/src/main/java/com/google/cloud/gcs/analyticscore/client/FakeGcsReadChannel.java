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

package com.google.cloud.gcs.analyticscore.client;

import com.google.cloud.ReadChannel;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.cloud.storage.Storage;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class FakeGcsReadChannel extends GcsReadChannel {
  private TrackingReadStrategy trackingStrategy;
  private Storage storage;

  public FakeGcsReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    super(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);
    this.storage = storage;
  }

  public FakeGcsReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    super(storage, itemId, readOptions, executorServiceSupplier, telemetry);
    this.storage = storage;
  }

  @Override
  protected ReadStrategy createReadStrategy(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      GcsItemInfo itemInfo,
      long position)
      throws IOException {
    ReadStrategy realStrategy =
        super.createReadStrategy(storage, itemId, readOptions, itemInfo, position);

    trackingStrategy = new TrackingReadStrategy(realStrategy);
    return trackingStrategy;
  }

  public TrackingReadStrategy getTrackingReadStrategy() {
    return trackingStrategy;
  }

  public TrackingReadChannel getTrackingReadChannel() {
    return trackingStrategy != null ? trackingStrategy.getCurrentChannel() : null;
  }

  public ReadChannel openSdkReadChannel(GcsItemId itemId, GcsReadOptions readOptions)
      throws IOException {
    ReadStrategy strategy = createReadStrategy(storage, itemId, readOptions, itemInfo, 0);
    return strategy.getReadChannel(0, 0);
  }

  public static int getOpenReadChannelCount() {
    return TrackingReadStrategy.getTotalGetReadChannelCalls();
  }

  public static void resetCounts() {
    TrackingReadStrategy.resetTotalGetReadChannelCalls();
  }
}
