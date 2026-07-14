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

import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsWriteOptions;
import com.google.cloud.storage.BlobId;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A unified OutputStream for writing objects to Google Cloud Storage.
 *
 * <p>This class wraps a WritableByteChannel (specifically GcsWriteChannel) to provide standard
 * java.io.OutputStream semantics.
 *
 * <p>Note: The {@code write} methods in this class are not thread-safe. It is the responsibility of
 * the calling layer (e.g., Iceberg's FileIO or Hadoop's HDFS implementations) to ensure thread
 * safety.
 */
public class GoogleCloudStorageOutputStream extends OutputStream {

  private final WritableByteChannel channel;

  // Used for single-byte writes to avoid repeated allocation.
  private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);
  private long bytesWritten = 0;

  /**
   * Creates a new instance of {@link GoogleCloudStorageOutputStream} for the given file info.
   *
   * @param gcsFileSystem the file system client to use
   * @param gcsFileInfo the file info identifying the object to write to
   * @return a new output stream
   * @throws IOException if an I/O error occurs
   */
  public static GoogleCloudStorageOutputStream create(
      GcsFileSystem gcsFileSystem, GcsFileInfo gcsFileInfo) throws IOException {
    checkNotNull(gcsFileSystem, "GcsFileSystem shouldn't be null");
    checkNotNull(gcsFileInfo, "GcsFileInfo shouldn't be null");
    return create(gcsFileSystem, gcsFileInfo.getItemInfo().getItemId());
  }

  /**
   * Creates a new instance of {@link GoogleCloudStorageOutputStream} for the given URI path.
   *
   * @param gcsFileSystem the file system client to use
   * @param path the GCS URI path (e.g., gs://bucket/object) identifying the object to write to
   * @return a new output stream
   * @throws IOException if an I/O error occurs
   */
  public static GoogleCloudStorageOutputStream create(GcsFileSystem gcsFileSystem, URI path)
      throws IOException {
    checkNotNull(gcsFileSystem, "GcsFileSystem shouldn't be null");
    BlobId blobId = BlobId.fromGsUtilUri(path.toString());
    GcsItemId itemId =
        GcsItemId.builder()
            .setBucketName(blobId.getBucket())
            .setObjectName(blobId.getName())
            .build();
    return create(gcsFileSystem, itemId);
  }

  /**
   * Creates a new instance of {@link GoogleCloudStorageOutputStream} for the given item ID.
   *
   * @param gcsFileSystem the file system client to use
   * @param itemId the item ID identifying the object to write to
   * @return a new output stream
   * @throws IOException if an I/O error occurs
   */
  public static GoogleCloudStorageOutputStream create(GcsFileSystem gcsFileSystem, GcsItemId itemId)
      throws IOException {
    checkNotNull(gcsFileSystem, "GcsFileSystem shouldn't be null");
    checkNotNull(itemId, "GcsItemId shouldn't be null");
    GcsWriteOptions writeOptions =
        gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsWriteOptions();
    WritableByteChannel channel = gcsFileSystem.create(itemId, writeOptions);
    return new GoogleCloudStorageOutputStream(channel);
  }

  private GoogleCloudStorageOutputStream(WritableByteChannel channel) {
    checkNotNull(channel, "WritableByteChannel shouldn't be null");
    this.channel = channel;
  }

  @Override
  public void write(int b) throws IOException {
    singleByteBuffer.clear();
    singleByteBuffer.put((byte) b);
    singleByteBuffer.flip();
    while (singleByteBuffer.hasRemaining()) {
      bytesWritten += channel.write(singleByteBuffer);
    }
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
    ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
    while (buffer.hasRemaining()) {
      bytesWritten += channel.write(buffer);
    }
  }

  @Override
  public void close() throws IOException {
    if (channel != null) {
      channel.close();
    }
  }

  /**
   * Returns the number of bytes written to this stream. Useful for systems like Apache Iceberg that
   * require a PositionOutputStream.
   *
   * @return the number of bytes written to this stream
   */
  public long getBytesWritten() {
    return bytesWritten;
  }
}
