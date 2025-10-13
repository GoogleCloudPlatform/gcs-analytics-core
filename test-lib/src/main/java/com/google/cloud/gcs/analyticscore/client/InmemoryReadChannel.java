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
import com.google.cloud.RestorableState;
import java.io.IOException;
import java.nio.ByteBuffer;

public class InmemoryReadChannel implements ReadChannel {
  public static final int BUFFER_CREATION_SEED = 1;

  private final ByteBuffer sourceBuffer;
  private boolean openStatus = true;

  private static int isOpenCount = 0;
  private static int closeCount = 0;
  private static int seekCount = 0;
  private static int setChunkSizeCount = 0;
  private static int captureCount = 0;
  private static int readCount = 0;

  InmemoryReadChannel(long fileSize) {
    this.sourceBuffer = getPredictableByteBuffer(fileSize);
  }

  @Override
  public boolean isOpen() {
    isOpenCount++;
    return openStatus;
  }

  @Override
  public void close() {
    closeCount++;
    openStatus = false;
  }

  @Override
  public void seek(long l) throws IOException {
    seekCount++;
    sourceBuffer.position(Math.toIntExact(l));
  }

  @Override
  public void setChunkSize(int i) {
    setChunkSizeCount++;
  }

  @Override
  public RestorableState<ReadChannel> capture() {
    captureCount++;
    return null;
  }

  @Override
  public int read(ByteBuffer byteBuffer) throws IOException {
    readCount++;
    if (!sourceBuffer.hasRemaining()) {
      return -1;
    }
    // Read bytes from inmemory buffer.
    int bytesToRead = Math.min(byteBuffer.remaining(), sourceBuffer.remaining());
    if (bytesToRead > 0) {
      int oldLimit = sourceBuffer.limit();
      sourceBuffer.limit(sourceBuffer.position() + bytesToRead);
      byteBuffer.put(sourceBuffer);
      sourceBuffer.limit(oldLimit);
    }
    return bytesToRead;
  }

  public static int getIsOpenCount() {
    return isOpenCount;
  }

  public static int getCloseCount() {
    return closeCount;
  }

  public static int getSeekCount() {
    return seekCount;
  }

  public static int getSetChunkSizeCount() {
    return setChunkSizeCount;
  }

  public static int getCaptureCount() {
    return captureCount;
  }

  public static int getReadCount() {
    return readCount;
  }

  public static void resetCounts() {
    isOpenCount = 0;
    closeCount = 0;
    seekCount = 0;
    setChunkSizeCount = 0;
    captureCount = 0;
    readCount = 0;
  }

  public static ByteBuffer getPredictableByteBuffer(long size) {
    return ByteBuffer.wrap(
        TestDataGenerator.generateSeededRandomBytes(Math.toIntExact(size), BUFFER_CREATION_SEED));
  }
}
