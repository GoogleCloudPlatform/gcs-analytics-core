/*
 * Copyright 2026 Google LLC
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

package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsWriteChannel;
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.storage.BlobInfo;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A unified OutputStream for writing objects to Google Cloud Storage.
 *
 * This class wraps a WritableByteChannel (specifically GcsWriteChannel) to provide standard
 * java.io.OutputStream semantics.
 */
public class GoogleCloudStorageOutputStream extends OutputStream {

  private final WritableByteChannel channel;

  // Used for single-byte writes to avoid repeated allocation.
  private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);

  /**
   * Creates a new GoogleCloudStorageOutputStream by initializing a write session via the client layer.
   */
  public static GoogleCloudStorageOutputStream create(
      GcsFileSystem gcsFileSystem, BlobInfo blobInfo, GcsWriteOptions writeOptions)
      throws IOException {
    checkState(gcsFileSystem != null, "GcsFileSystem shouldn't be null");
    checkState(blobInfo != null, "BlobInfo shouldn't be null");
    checkState(writeOptions != null, "GcsWriteOptions shouldn't be null");
    WritableByteChannel channel = gcsFileSystem.create(blobInfo, writeOptions);
    return new GoogleCloudStorageOutputStream(channel);
  }

  private GoogleCloudStorageOutputStream(WritableByteChannel channel) {
    this.channel = channel;
  }

  @Override
  public void write(int b) throws IOException {
    singleByteBuffer.clear();
    singleByteBuffer.put((byte) b);
    singleByteBuffer.flip();
    channel.write(singleByteBuffer);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkNotNull(b, "buffer must not be null");
    if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return;
    }

    // Wrap the byte array and pass it to the GcsWriteChannel in the client module
    channel.write(ByteBuffer.wrap(b, off, len));
  }

  @Override
  public void close() throws IOException {
    if (channel != null && channel.isOpen()) {
      channel.close();
    }
  }

  /**
   * Returns the number of bytes written to this stream.
   * Useful for systems like Apache Iceberg that require a PositionOutputStream.
   */
  public long getBytesWritten() {
    if (channel instanceof GcsWriteChannel) {
      return ((GcsWriteChannel) channel).getBytesWritten();
    }
    return 0; // Fallback if a different channel implementation is used
  }
}
