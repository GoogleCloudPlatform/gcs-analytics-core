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

import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class FakeGcsClientImpl implements GcsClient {
  private final Supplier<ExecutorService> executorServiceSupplier;
  private final Map<GcsItemId, Long> itemIdToSizeMap;

  private static int openReadChannelCount = 0;
  private static int getGcsItemInfoCount = 0;
  private static int closeCount = 0;

  public FakeGcsClientImpl(
      Map<GcsItemId, Long> itemIdToSizeMap, Supplier<ExecutorService> executorServiceSupplier) {
    this.itemIdToSizeMap = itemIdToSizeMap;
    this.executorServiceSupplier = executorServiceSupplier;
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemInfo itemInfo, GcsReadOptions readOptions) throws IOException {
    openReadChannelCount++;
    return new FakeGcsReadChannel(
        StorageOptions.newBuilder().build().getService(),
        itemInfo,
        readOptions,
        executorServiceSupplier);
  }

  @Override
  public GcsItemInfo getGcsItemInfo(GcsItemId itemId) throws IOException {
    getGcsItemInfoCount++;
    if (!itemIdToSizeMap.containsKey(itemId)) {
      throw new IOException("Object not found:" + itemId);
    }
    if (itemId.isGcsObject()) {
      return GcsItemInfo.builder().setItemId(itemId).setSize(itemIdToSizeMap.get(itemId)).build();
    }
    throw new UnsupportedOperationException(
        String.format("Expected gcs object but got %s", itemId));
  }

  @Override
  public void close() {
    closeCount++;
  }

  public static int getOpenReadChannelCount() {
    return openReadChannelCount;
  }

  public static int getGetGcsItemInfoCount() {
    return getGcsItemInfoCount;
  }

  public static int getCloseCount() {
    return closeCount;
  }

  public static void resetCounts() {
    openReadChannelCount = 0;
    getGcsItemInfoCount = 0;
    closeCount = 0;
  }
}
