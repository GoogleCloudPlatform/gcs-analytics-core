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

package com.google.cloud.gcs.analyticscore.core.optimizer;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsCacheOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

/** A {@link FormatOptimizer} that caches and serves small objects in a private buffer. */
public class SmallObjectOptimizer implements FormatOptimizer {

  private static final Set<String> DATA_FILE_EXTENSIONS = Set.of(".parquet", ".orc");

  private final GcsCacheOptions cacheOptions;
  private final GcsReadOptions readOptions;
  private final Telemetry telemetry;

  private AnalyticsCacheManager cacheManager;
  private GcsItemId currentItemId;
  private long fileSize = -1;
  private ByteBuffer localCachedBuffer;

  public SmallObjectOptimizer(
      GcsCacheOptions cacheOptions, GcsReadOptions readOptions, Telemetry telemetry) {
    this.cacheOptions = checkNotNull(cacheOptions, "cacheOptions cannot be null");
    this.readOptions = checkNotNull(readOptions, "readOptions cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return false; // File size is required to determine applicability
  }

  @Override
  public boolean isApplicable(GcsFileInfo fileInfo) {
    return cacheOptions.isSmallObjectCacheEnabled()
        && fileInfo.getItemInfo().getSize() <= readOptions.getSmallObjectCacheThresholdBytes()
        && fileInfo
            .getItemInfo()
            .getItemId()
            .getObjectName()
            .map(
                name ->
                    DATA_FILE_EXTENSIONS.stream().anyMatch(ext -> name.toLowerCase().endsWith(ext)))
            .orElse(false);
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.currentItemId = itemId;
    this.cacheManager = cacheManager;
  }

  @Override
  public void onOpen(GcsFileInfo fileInfo, AnalyticsCacheManager cacheManager) {
    this.currentItemId = fileInfo.getItemInfo().getItemId();
    this.cacheManager = cacheManager;
    this.fileSize = fileInfo.getItemInfo().getSize();
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    if (fileSize == -1 || fileSize > readOptions.getSmallObjectCacheThresholdBytes()) {
      return 0;
    }

    if (position >= fileSize) {
      return -1;
    }

    if (localCachedBuffer == null) {
      AtomicBoolean isMiss = new AtomicBoolean(false);
      localCachedBuffer =
          cacheManager.getSmallObject(
              currentItemId,
              itemId -> {
                isMiss.set(true);
                return ensureCached(source);
              });

      telemetry.recordMetric(
          isMiss.get() ? Metric.SMALL_OBJECT_CACHE_MISS : Metric.SMALL_OBJECT_CACHE_HIT,
          1L,
          Collections.emptyMap());
    }

    return serveFromCache(position, dst, localCachedBuffer);
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges, IntFunction<ByteBuffer> allocate) throws IOException {
    if (fileSize == -1 || fileSize > readOptions.getSmallObjectCacheThresholdBytes()) {
      return ranges;
    }

    ByteBuffer cachedBuffer;
    try {
      cachedBuffer =
          cacheManager.getSmallObject(
              currentItemId,
              id -> {
                throw new IOException("Cache miss during vectored read");
              });
    } catch (IOException e) {
      return ranges; // Cannot satisfy yet
    }

    telemetry.recordMetric(Metric.SMALL_OBJECT_CACHE_HIT, ranges.size(), Collections.emptyMap());
    for (GcsObjectRange range : ranges) {
      ByteBuffer dest = allocate.apply(range.getLength());
      if (dest == null) {
        range
            .getByteBufferFuture()
            .completeExceptionally(
                new IllegalArgumentException(
                    String.format("Buffer allocation returned null for range: %s", range)));
        continue;
      }
      int bytesRead = serveFromCache(range.getOffset(), dest, cachedBuffer);
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
    return Collections.emptyList();
  }

  private ByteBuffer ensureCached(VectoredSeekableByteChannel source) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
    long originalPosition = source.position();
    try {
      source.position(0);
      while (buffer.hasRemaining()) {
        if (source.read(buffer) == -1) {
          throw new IOException("Unexpected EOF encountered while reading small object.");
        }
      }
      buffer.flip();
      return buffer;
    } finally {
      source.position(originalPosition);
    }
  }

  private int serveFromCache(long currPosition, ByteBuffer buffer, ByteBuffer cachedBuffer) {
    if (currPosition >= fileSize) {
      return -1;
    }
    ByteBuffer cacheView = cachedBuffer.duplicate();
    cacheView.position((int) currPosition);
    int bytesToRead = Math.min(buffer.remaining(), cacheView.remaining());
    cacheView.limit(cacheView.position() + bytesToRead);
    buffer.put(cacheView);
    return bytesToRead;
  }
}
