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
import com.google.cloud.storage.Storage;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class FakeGcsReadChannel extends GcsReadChannel {
  private static int openReadChannelCount = 0;

  public FakeGcsReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier)
      throws IOException {
    super(storage, itemInfo, readOptions, executorServiceSupplier);
  }

  @Override
  protected ReadChannel openReadChannel(GcsItemInfo itemInfo, GcsReadOptions readOptions)
      throws IOException {
    openReadChannelCount++;
    return super.openReadChannel(itemInfo, readOptions);
  }

  public static int getOpenReadChannelCount() {
    return openReadChannelCount;
  }

  public static void resetCounts() {
    openReadChannelCount = 0;
  }
}
