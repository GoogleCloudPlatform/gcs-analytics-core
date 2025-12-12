/*
 * Copyright 2025 Google LLC
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
package com.google.cloud.gcs.analyticscore.client;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.ExecutorService;

class GcsSimpleReadChannel extends GcsReadChannel {

  private final ReadChannel readChannel;

  GcsSimpleReadChannel(
      Storage storage,
      GcsItemInfo itemInfo,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier)
      throws IOException {
    super(storage, itemInfo, readOptions, executorServiceSupplier);
    this.readChannel = openReadChannel(itemId, readOptions);
  }

  GcsSimpleReadChannel(
      Storage storage,
      GcsItemId itemId,
      GcsReadOptions readOptions,
      Supplier<ExecutorService> executorServiceSupplier)
      throws IOException {
    super(storage, itemId, readOptions, executorServiceSupplier);
    this.readChannel = openReadChannel(itemId, readOptions);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int bytesRead = readChannel.read(dst);
    position += bytesRead;

    return bytesRead;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    validatePosition(newPosition);
    readChannel.seek(newPosition);
    position = newPosition;

    return this;
  }

  @Override
  public boolean isOpen() {
    return readChannel.isOpen();
  }

  @Override
  public void close() throws IOException {
    if (readChannel.isOpen()) {
      readChannel.close();
    }
  }
}
