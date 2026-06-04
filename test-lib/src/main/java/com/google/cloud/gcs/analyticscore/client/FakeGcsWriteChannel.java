/*
 * Copyright 2026 Google LLC
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

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobWriteSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class FakeGcsWriteChannel extends GcsWriteChannel {
  private static int writeCallCount = 0;
  private static int closeCallCount = 0;

  public FakeGcsWriteChannel(
      BlobWriteSession blobWriteSession,
      WritableByteChannel sdkWriteChannel,
      BlobInfo blobInfo,
      GcsWriteOptions writeOptions) {
    super(blobWriteSession, sdkWriteChannel, blobInfo, writeOptions);
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    writeCallCount++;
    return super.write(src);
  }

  @Override
  public void close() throws IOException {
    closeCallCount++;
    super.close();
  }

  public static int getWriteCallCount() {
    return writeCallCount;
  }

  public static int getCloseCallCount() {
    return closeCallCount;
  }

  public static void resetCounts() {
    writeCallCount = 0;
    closeCallCount = 0;
  }
}
