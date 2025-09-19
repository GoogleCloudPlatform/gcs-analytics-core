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
package com.google.cloud.gcs.analyticscore.core;

import static com.google.common.base.Preconditions.*;

import com.google.cloud.gcs.analyticscore.client.*;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is a seekable input stream for GCS objects. It is backed by a GcsFileSystem instance. */
public class GoogleCloudStorageInputStream extends SeekableInputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleCloudStorageInputStream.class);

  // Used for single-byte reads to avoid repeated allocation.
  private final ByteBuffer singleByteBuffer = ByteBuffer.wrap(new byte[1]);

  private final GcsFileSystem gcsFileSystem;
  private final VectoredSeekableByteChannel channel;
  private long position;
  private final URI gcsPath;
  private final long fileSize;

  private volatile boolean closed;

  // Footer cache fields
  private volatile ByteBuffer footerCache;
  private final long prefetchSize;

  public static GoogleCloudStorageInputStream create(GcsFileSystem gcsFileSystem, URI path)
      throws IOException {
    checkState(gcsFileSystem != null, "GcsFileSystem shouldn't be null");
    VectoredSeekableByteChannel channel =
        gcsFileSystem.open(
            path, gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions());
    return new GoogleCloudStorageInputStream(gcsFileSystem, channel, path);
  }

  private GoogleCloudStorageInputStream(
      GcsFileSystem gcsFileSystem, VectoredSeekableByteChannel channel, URI path)
      throws IOException {
    this.gcsFileSystem = gcsFileSystem;
    this.channel = channel;
    this.position = 0;
    this.gcsPath = path;
    this.prefetchSize =
        gcsFileSystem
            .getFileSystemOptions()
            .getGcsClientOptions()
            .getGcsReadOptions()
            .getFooterPrefetchSize();
    if (channel != null) {
      this.fileSize = channel.size();
    } else {
      fileSize = 0;
    }
  }

  // TODO(shubhamdiwakar): Performance test the lazy seek approach with a separate channel.
  private void cacheFooter() throws IOException {
    long originalPosition = -1;
    try {
      originalPosition = getPos();
      // File is too small to store footer.
      // TODO(shubhamdiwakar): Implement Small object caching.
      if (prefetchSize >= fileSize) {
        return;
      }
      long footerCacheStartPosition = fileSize - prefetchSize;
      LOG.debug(
          "Caching footer for {}. Position: {}, Size: {}",
          gcsPath,
          footerCacheStartPosition,
          prefetchSize);
      ByteBuffer footerByteBuffer = ByteBuffer.allocate((int) prefetchSize);
      channel.position(footerCacheStartPosition);

      while (footerByteBuffer.hasRemaining()) {
        if (channel.read(footerByteBuffer) == -1) {
          LOG.warn("Unexpected EOF while caching footer for {}", gcsPath);
          break;
        }
      }
      footerByteBuffer.flip();
      this.footerCache = footerByteBuffer;
      LOG.debug("Cached {} bytes of footer for {}", footerCache.remaining(), gcsPath);
    } catch (IOException e) {
      // Log the error but don't fail the operation as this improves performance. The read will fall
      // back to the main channel.
      LOG.warn(
          "Failed to cache footer for {}. Falling back to standard read. Error: {}",
          gcsPath,
          e.getMessage());
    } finally {
      seek(originalPosition);
    }
  }

  private int serveFromFooterCache(ByteBuffer buffer) throws IOException {
    // TODO(shubhamdiwakar): Refactor to use GcsFileInfo instead of fileSize.
    long footerCacheStartPosition = fileSize - prefetchSize;
    ByteBuffer cacheView = footerCache.duplicate();
    cacheView.position((int) (position - footerCacheStartPosition));
    int bytesToRead = Math.min(buffer.remaining(), cacheView.remaining());
    if (bytesToRead > 0) {
      cacheView.limit(cacheView.position() + bytesToRead);
      buffer.put(cacheView);
    }
    seek(position + bytesToRead);
    return bytesToRead;
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public void seek(long newPos) throws IOException {
    checkArgument(newPos >= 0, "position can't be negative: %s", newPos);
    checkNotClosed("Cannot seek: already closed");
    position = newPos;
    channel.position(newPos);
  }

  @Override
  public int read() throws IOException {
    checkNotClosed("Cannot read: already closed");
    // Delegate to the byte array read method to reuse the cache logic.
    int bytesRead = read(singleByteBuffer.array(), 0, 1);
    if (bytesRead == -1) {
      return -1;
    }
    return singleByteBuffer.array()[0] & 0xFF;
  }

  public int read(ByteBuffer byteBuffer) throws IOException {
    checkNotClosed("Cannot read: already closed");

    if (footerCache == null && prefetchSize > 0 && position >= fileSize - prefetchSize) {
      cacheFooter();
    }
    // If the footer is cached and the read is within its range, serve from the cache.
    if (footerCache != null && position >= fileSize - prefetchSize) {
      return serveFromFooterCache(byteBuffer);
    }

    long channelPosition = channel.position();
    checkState(
            channelPosition == position,
            "Channel position (%s) and stream position (%s) should be the same",
            channelPosition,
            position);

    int bytesRead = channel.read(byteBuffer);
    if (bytesRead > 0) {
      position += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public int read(@Nonnull byte[] buffer, int offset, int length) throws IOException {
    checkNotClosed("Cannot read: already closed");
    checkNotNull(buffer, "buffer must not be null");

    if (offset < 0 || length < 0 || length > buffer.length - offset) {
      throw new IndexOutOfBoundsException();
    }
    if (length == 0) {
      return 0;
    }
    return read(ByteBuffer.wrap(buffer, offset, length));
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      if (channel != null) {
        channel.close();
      }
    }
  }

  private void checkNotClosed(String msg) throws IOException {
    if (closed) {
      throw new IOException(gcsPath + ": " + msg);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    try (VectoredSeekableByteChannel byteChannel =
        gcsFileSystem.open(
            gcsPath,
            gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions())) {
      byteChannel.position(position);
      int numberOfBytesRead = byteChannel.read(ByteBuffer.wrap(buffer, offset, length));
      if (numberOfBytesRead < length) {
        throw new EOFException(
            "Reached the end of stream with "
                + (length - numberOfBytesRead)
                + " bytes left to read");
      }
    }
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    try (VectoredSeekableByteChannel byteChannel =
        gcsFileSystem.open(
            gcsPath,
            gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions())) {
      GcsFileInfo gcsFileInfo = gcsFileSystem.getFileInfo(gcsPath);
      long size = gcsFileInfo.getItemInfo().getSize();
      long startPosition = Math.max(0, size - offset);
      byteChannel.position(startPosition);
      return byteChannel.read(ByteBuffer.wrap(buffer, offset, length));
    }
  }

  @Override
  public void readVectored(List<GcsObjectRange> fileRanges, IntFunction<ByteBuffer> alloc)
      throws IOException {
    channel.readVectored(fileRanges, alloc);
  }
}
