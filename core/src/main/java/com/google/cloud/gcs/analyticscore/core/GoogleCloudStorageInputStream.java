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

  private static final int LARGE_FILE_SIZE_THRESHOLD = 1024 * 1024 * 1024; // 1 GB.
  // Used for single-byte reads to avoid repeated allocation.
  private final ByteBuffer singleByteBuffer = ByteBuffer.wrap(new byte[1]);

  private final GcsFileSystem gcsFileSystem;
  private final VectoredSeekableByteChannel channel;
  private long position;
  private final URI gcsPath;
  private final long fileSize;
  private final GcsFileInfo gcsFileInfo;

  private volatile boolean closed;

  // Unified cache for small objects or footers.
  private final long prefetchSize;
  private volatile ByteBuffer prefetchBuffer;

  public static GoogleCloudStorageInputStream create(
      GcsFileSystem gcsFileSystem, GcsFileInfo gcsFileInfo) throws IOException {
    checkState(gcsFileInfo != null, "GcsFileInfo shouldn't be null");
    VectoredSeekableByteChannel channel =
        gcsFileSystem.open(
            gcsFileInfo,
            gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions());
    return new GoogleCloudStorageInputStream(gcsFileSystem, channel, gcsFileInfo);
  }

  public static GoogleCloudStorageInputStream create(GcsFileSystem gcsFileSystem, URI path)
      throws IOException {
    checkState(gcsFileSystem != null, "GcsFileSystem shouldn't be null");
    GcsFileInfo fileInfo = gcsFileSystem.getFileInfo(path);
    return create(gcsFileSystem, fileInfo);
  }

  private GoogleCloudStorageInputStream(
      GcsFileSystem gcsFileSystem, VectoredSeekableByteChannel channel, GcsFileInfo gcsFileInfo) {
    this.gcsFileSystem = gcsFileSystem;
    this.channel = channel;
    this.position = 0;
    this.gcsPath = gcsFileInfo.getUri();
    this.gcsFileInfo = gcsFileInfo;
    this.fileSize = gcsFileInfo.getItemInfo().getSize();
    GcsReadOptions readOptions =
        gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions();
    this.prefetchSize = calculatePrefetchSize(fileSize, readOptions);
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
    if (prefetchBuffer == null && position >= fileSize - prefetchSize) {
      cacheObjectOrFooter();
    }
    if (prefetchBuffer != null && (position >= fileSize - prefetchSize)) {
      return serveFromCache(byteBuffer);
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
            gcsFileInfo,
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
            gcsFileInfo,
            gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions())) {
      long size = gcsFileInfo.getItemInfo().getSize();
      long startPosition = Math.max(0, size - offset);
      byteChannel.position(startPosition);
      return byteChannel.read(ByteBuffer.wrap(buffer, offset, length));
    }
  }

  @Override
  public void readVectored(List<GcsObjectRange> fileRanges, IntFunction<ByteBuffer> alloc)
      throws IOException {
    if (prefetchBuffer != null && prefetchSize == fileSize) {
      // Entire object is cached, serve from prefetchBuffer
      for (GcsObjectRange range : fileRanges) {
        ByteBuffer dest = alloc.apply(range.getLength());
        int bytesRead = serveFromCacheWithoutSeek(range.getOffset(), dest);
        if (bytesRead < range.getLength()) {
          range
              .getByteBufferFuture()
              .completeExceptionally(
                  new EOFException(
                      String.format("Error while populating range: %s, unexpected EOF", range)));
        } else {
          dest.flip();
          range.getByteBufferFuture().complete(dest);
        }
      }
    } else {
      channel.readVectored(fileRanges, alloc);
    }
  }

  private void cacheObjectOrFooter() throws IOException {
    long originalPosition = getPos();
    long startPosition = fileSize - prefetchSize;
    int bufferSize = (int) (fileSize - startPosition);
    LOG.debug(
        "Caching GCS object {} from position: {} size: {}", gcsPath, startPosition, bufferSize);
    try {
      ByteBuffer cacheBuffer = ByteBuffer.allocate(bufferSize);
      channel.position(startPosition);
      while (cacheBuffer.hasRemaining()) {
        if (channel.read(cacheBuffer) == -1) {
          throw new IOException("Unexpected EOF encountered.");
        }
      }
      cacheBuffer.flip();
      this.prefetchBuffer = cacheBuffer;
    } catch (IOException e) {
      LOG.warn(
          "Error while caching object {} from position: {} length: {}. Error : {}",
          gcsPath,
          startPosition,
          bufferSize,
          e.getMessage());
    } finally {
      seek(originalPosition);
    }
  }

  private int serveFromCache(ByteBuffer buffer) throws IOException {
    int bytesToRead = serveFromCacheWithoutSeek(position, buffer);
    if (bytesToRead != -1) {
      seek(position + bytesToRead);
    }
    return bytesToRead;
  }

  private int serveFromCacheWithoutSeek(long currPosition, ByteBuffer buffer) throws IOException {
    ByteBuffer cacheView = prefetchBuffer.duplicate();
    int readStartPosition = (int) (currPosition - (fileSize - prefetchSize));
    cacheView.position(readStartPosition);
    int bytesToRead = buffer.remaining();
    if (bytesToRead > cacheView.remaining()) {
      // Cache is expected to contain data till end of stream, if this happens don't read from
      // cache.
      // Signal End of stream.
      return -1;
    }
    cacheView.limit(cacheView.position() + bytesToRead);
    buffer.put(cacheView);
    return bytesToRead;
  }

  private static long calculatePrefetchSize(long fileSize, GcsReadOptions readOptions) {
    if (!readOptions.isFooterPrefetchEnabled()
        && readOptions.getSmallObjectCacheSize() < fileSize) {
      // Both footer prefetch and small object cache are disabled.
      return 0;
    }
    if (readOptions.getSmallObjectCacheSize() >= fileSize) {
      // Small object cache is enabled and file size is <= the cache size.
      return fileSize;
    }
    // Footer prefetch.
    return fileSize > LARGE_FILE_SIZE_THRESHOLD
        ? Math.min(readOptions.getFooterPrefetchSizeLargeFile(), fileSize)
        : Math.min(readOptions.getFooterPrefetchSizeSmallFile(), fileSize);
  }
}
