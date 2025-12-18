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
import com.google.cloud.gcs.analyticscore.common.telemetry.GcsAnalyticsCoreMetrics;
import com.google.cloud.gcs.analyticscore.common.telemetry.GcsAnalyticsCoreTelemetry;
import com.google.cloud.gcs.analyticscore.common.telemetry.GcsOperation;
import com.google.cloud.gcs.analyticscore.common.telemetry.GcsOperationType;
import com.google.cloud.gcs.analyticscore.common.telemetry.TelemetryAttributes;
import com.google.cloud.storage.BlobId;
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
  private GcsItemId gcsItemId;

  private volatile boolean closed;

  // Unified cache for small objects or footers.
  private long prefetchSize;
  private long fileSize;
  private volatile ByteBuffer prefetchBuffer;
  private TelemetryAttributes telemetryAttributes;

  private GcsFileInfo gcsFileInfo;

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

  public static GoogleCloudStorageInputStream create(GcsFileSystem gcsFileSystem, GcsItemId itemId)
      throws IOException {
    checkState(gcsFileSystem != null, "GcsFileSystem shouldn't be null");
    VectoredSeekableByteChannel channel =
        gcsFileSystem.open(
            itemId, gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions());
    return new GoogleCloudStorageInputStream(gcsFileSystem, channel, itemId);
  }

  private GoogleCloudStorageInputStream(
      GcsFileSystem gcsFileSystem, VectoredSeekableByteChannel channel, GcsFileInfo gcsFileInfo) {
    this(gcsFileSystem, channel, gcsFileInfo.getItemInfo().getItemId());
    initializeMetadata(gcsFileInfo);
  }

  private GoogleCloudStorageInputStream(
      GcsFileSystem gcsFileSystem, VectoredSeekableByteChannel channel, GcsItemId itemId) {
    this.gcsFileSystem = gcsFileSystem;
    this.channel = channel;
    this.gcsPath =
        URI.create(BlobId.of(itemId.getBucketName(), itemId.getObjectName().get()).toGsUtilUri());
    this.gcsItemId = itemId;
    this.position = 0;
    this.telemetryAttributes =
        TelemetryAttributes.builder()
            .setClassName(GoogleCloudStorageInputStream.class.getName())
            .build();
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public void seek(long newPos) throws IOException {
    GcsAnalyticsCoreTelemetry.getInstance()
        .measure(
            GcsOperation.builder()
                .setAttributes(telemetryAttributes)
                .setType(GcsOperationType.SEEK)
                .build(),
            recorder -> {
              checkArgument(newPos >= 0, "position can't be negative: %s", newPos);
              checkNotClosed("Cannot seek: already closed");
              position = newPos;
              channel.position(newPos);
              recorder.record(
                  GcsAnalyticsCoreMetrics.GCS_SEEK_DISTANCE,
                  newPos - position,
                  telemetryAttributes);
              return null;
            });
  }

  @Override
  public int read() throws IOException {
    return GcsAnalyticsCoreTelemetry.getInstance()
        .measure(
            GcsOperation.builder()
                .setType(GcsOperationType.READ)
                .setAttributes(telemetryAttributes)
                .setDurationMetricName(GcsAnalyticsCoreMetrics.GCS_READ_DURATION)
                .build(),
            recorder -> {
              checkNotClosed("Cannot read: already closed");
              // Delegate to the byte array read method to reuse the cache logic.
              int bytesRead = read(singleByteBuffer.array(), 0, 1);
              if (bytesRead == -1) {
                return -1;
              }
              recorder.record(
                  GcsAnalyticsCoreMetrics.GCS_BYTES_READ, bytesRead, telemetryAttributes);

              return singleByteBuffer.array()[0] & 0xFF;
            });
  }

  @Override
  public int read(ByteBuffer byteBuffer) throws IOException {
    return GcsAnalyticsCoreTelemetry.getInstance()
        .measure(
            GcsOperation.builder()
                .setType(GcsOperationType.READ)
                .setDurationMetricName(GcsAnalyticsCoreMetrics.GCS_READ_DURATION)
                .build(),
            recorder -> {
              TelemetryAttributes metricAttributes =
                  telemetryAttributes.toBuilder()
                      .setReadLength(byteBuffer.remaining())
                      .setReadOffset(byteBuffer.position())
                      .build();
              checkNotClosed("Cannot read: already closed");
              if (isMetadataInitialized()
                  && prefetchBuffer == null
                  && position >= fileSize - prefetchSize) {
                cacheObjectOrFooter();
              }
              if (prefetchBuffer != null && (position >= fileSize - prefetchSize)) {
                int bytesRead = serveFromCache(byteBuffer);
                if (bytesRead > 0) {
                  recorder.record(
                      GcsAnalyticsCoreMetrics.GCS_BYTES_READ, bytesRead, metricAttributes);
                  recorder.record(
                      GcsAnalyticsCoreMetrics.GCS_READ_CACHE_HIT_COUNT, 1, metricAttributes);
                }
                return bytesRead;
              }
              recorder.record(
                  GcsAnalyticsCoreMetrics.GCS_READ_CACHE_MISS_COUNT, 1, metricAttributes);
              long channelPosition = channel.position();
              checkState(
                  channelPosition == position,
                  "Channel position (%s) and stream position (%s) should be the same",
                  channelPosition,
                  position);

              int bytesRead = channel.read(byteBuffer);
              if (bytesRead > 0) {
                position += bytesRead;
                recorder.record(
                    GcsAnalyticsCoreMetrics.GCS_BYTES_READ, bytesRead, metricAttributes);
              }
              return bytesRead;
            });
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
    GcsAnalyticsCoreTelemetry.getInstance()
        .measure(
            GcsOperation.builder().setType(GcsOperationType.CLOSE).build(),
            recorder -> {
              if (!closed) {
                closed = true;
                if (channel != null) {
                  channel.close();
                }
              }
              return null;
            });
  }

  private void checkNotClosed(String msg) throws IOException {
    if (closed) {
      throw new IOException(gcsPath + ": " + msg);
    }
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    GcsAnalyticsCoreTelemetry.getInstance()
        .measure(
            GcsOperation.builder()
                .setType(GcsOperationType.READ)
                .setDurationMetricName(GcsAnalyticsCoreMetrics.GCS_READ_DURATION)
                .build(),
            recorder -> {
              try (VectoredSeekableByteChannel byteChannel = openReadChannel()) {
                byteChannel.position(position);
                int numberOfBytesRead = byteChannel.read(ByteBuffer.wrap(buffer, offset, length));
                if (numberOfBytesRead < length) {
                  throw new EOFException(
                      "Reached the end of stream with "
                          + (length - numberOfBytesRead)
                          + " bytes left to read");
                }
                recorder.record(
                    GcsAnalyticsCoreMetrics.GCS_BYTES_READ, numberOfBytesRead, telemetryAttributes);
              }
              return null;
            });
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    return GcsAnalyticsCoreTelemetry.getInstance()
        .measure(
            GcsOperation.builder()
                .setType(GcsOperationType.READ)
                .setDurationMetricName(GcsAnalyticsCoreMetrics.GCS_READ_DURATION)
                .build(),
            recorder -> {
              if (!isMetadataInitialized()) {
                initializeMetadata();
              }
              try (VectoredSeekableByteChannel byteChannel = openReadChannel()) {
                long size = gcsFileInfo.getItemInfo().getSize();
                long startPosition = Math.max(0, size - length);
                byteChannel.position(startPosition);
                int bytesRead = byteChannel.read(ByteBuffer.wrap(buffer, offset, length));
                if (bytesRead > 0) {
                  recorder.record(
                      GcsAnalyticsCoreMetrics.GCS_BYTES_READ, bytesRead, telemetryAttributes);
                }
                return bytesRead;
              }
            });
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

  private VectoredSeekableByteChannel openReadChannel() throws IOException {
    if (gcsFileInfo != null) {
      return gcsFileSystem.open(
          gcsFileInfo,
          gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions());
    }
    return gcsFileSystem.open(
        gcsItemId, gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions());
  }

  private boolean isMetadataInitialized() {
    return gcsFileInfo != null;
  }

  private void initializeMetadata() throws IOException {
    initializeMetadata(gcsFileSystem.getFileInfo(gcsItemId));
  }

  private void initializeMetadata(GcsFileInfo fileInfo) {
    this.gcsFileInfo = fileInfo;
    this.gcsItemId = fileInfo.getItemInfo().getItemId();
    this.fileSize = fileInfo.getItemInfo().getSize();
    GcsReadOptions readOptions =
        gcsFileSystem.getFileSystemOptions().getGcsClientOptions().getGcsReadOptions();
    this.prefetchSize = calculatePrefetchSize(fileSize, readOptions);
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
    if (cacheView.remaining() == 0) {
      return -1;
    }
    int bytesToRead = Math.min(buffer.remaining(), cacheView.remaining());
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
