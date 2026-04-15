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
  private static int openReadChannelCount = 0;
  private TrackingReadChannel trackingReadChannel;
  private int defaultEofAtCall = -1;

  public FakeGcsReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    super(storage, itemInfo, readOptions, executorServiceSupplier, telemetry);
  }

  public FakeGcsReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier,
      Telemetry telemetry)
      throws IOException {
    super(storage, itemId, readOptions, executorServiceSupplier, telemetry);
  }

  public void setDefaultEofAtCall(int eofAtCall) {
    this.defaultEofAtCall = eofAtCall;
  }

  @Override
  protected ReadChannel openSdkReadChannel(GcsItemId itemId, GcsReadOptions readOptions)
      throws IOException {
    openReadChannelCount++;
    ReadChannel delegate = super.openSdkReadChannel(itemId, readOptions);
    trackingReadChannel = new TrackingReadChannel(delegate);
    if (defaultEofAtCall != -1) {
      trackingReadChannel.setEofAtCall(defaultEofAtCall);
    }
    return trackingReadChannel;
  }

  public TrackingReadChannel getTrackingReadChannel() {
    return trackingReadChannel;
  }

  public static int getOpenReadChannelCount() {
    return openReadChannelCount;
  }

  public static void resetCounts() {
    openReadChannelCount = 0;
  }
}
