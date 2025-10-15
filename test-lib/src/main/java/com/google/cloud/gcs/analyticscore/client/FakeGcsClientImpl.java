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

import com.google.auth.Credentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class FakeGcsClientImpl extends GcsClientImpl {
  public static Storage storage = LocalStorageHelper.getOptions().getService();

  private static int openReadChannelCount = 0;
  private static int closeCount = 0;

  FakeGcsClientImpl(
      Credentials credentials,
      GcsClientOptions clientOptions,
      Supplier<ExecutorService> executorServiceSupplier) {
    super(credentials, clientOptions, executorServiceSupplier);
  }

  FakeGcsClientImpl(
      GcsClientOptions clientOptions, Supplier<ExecutorService> executorServiceSupplier) {
    super(clientOptions, executorServiceSupplier);
  }

  @Override
  protected Storage createStorage(Optional<Credentials> credentials) {
    return storage;
  }

  @Override
  public VectoredSeekableByteChannel openReadChannel(
      GcsItemInfo itemInfo, GcsReadOptions readOptions) throws IOException {
    openReadChannelCount++;
    return super.openReadChannel(itemInfo, readOptions);
  }

  @Override
  public void close() {
    closeCount++;
    super.close();
  }

  public static int getOpenReadChannelCount() {
    return openReadChannelCount;
  }

  public static int getCloseCount() {
    return closeCount;
  }

  public static void resetCounts() {
    openReadChannelCount = 0;
    closeCount = 0;
  }
}
