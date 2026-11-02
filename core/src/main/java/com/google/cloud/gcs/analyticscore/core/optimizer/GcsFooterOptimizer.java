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
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.common.GcsAnalyticsCoreTelemetryConstants.Metric;
import com.google.cloud.gcs.analyticscore.common.telemetry.Telemetry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/** A {@link FormatOptimizer} that caches and serves GCS object footers (e.g., for Parquet). */
public class GcsFooterOptimizer implements FormatOptimizer {

  private static final int LARGE_FILE_SIZE_THRESHOLD = 1024 * 1024 * 1024; // 1 GB
  private static final ImmutableList<String> FOOTER_OPTIMIZABLE_EXTENSIONS =
      ImmutableList.of(
          ".parquet", ".orc", ".hudi", ".hfile", ".lance", ".vortex", ".feather", ".arrow");
  private static final long MAX_SPECULATIVE_PREFETCH_SIZE = 4L * 1024 * 1024; // 4 MiB

  private final GcsReadOptions readOptions;
  private final Telemetry telemetry;

  private AnalyticsCacheManager cacheManager;
  private GcsItemId gcsItemId;
  private long fileSize = -1;
  private long prefetchSize = -1;
  private ByteBuffer localFooterBuffer;
  private long localBufferStartPosition = -1;
  private boolean speculativeReadAttempted = false;
  private ByteBuffer pendingSpeculativeBuffer;
  private long pendingSpeculativeStartPosition = -1;

  public GcsFooterOptimizer(GcsReadOptions readOptions, Telemetry telemetry) {
    this.readOptions = checkNotNull(readOptions, "readOptions cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return readOptions.isFooterPrefetchEnabled()
        && DataFileExtensions.hasAnyExtension(
            itemId.getObjectName(), FOOTER_OPTIMIZABLE_EXTENSIONS);
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.gcsItemId = itemId;
    this.cacheManager = cacheManager;
  }

  @Override
  public void onOpen(GcsFileInfo fileInfo, AnalyticsCacheManager cacheManager) {
    this.gcsItemId = fileInfo.getItemInfo().getItemId();
    this.cacheManager = cacheManager;
    this.fileSize = fileInfo.getItemInfo().getSize();
    this.prefetchSize = calculatePrefetchSize(fileSize, readOptions);
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    if (fileSize == -1) {
      resolveFileSizeFromItemInfo(source);
    }
    if (fileSize == -1) {
      if (!speculativeReadAttempted) {
        return readWithUnknownFileSize(position, dst, source);
      }
      resolveFileSizeExplicitly(source);
    }
    adoptPendingSpeculativeBuffer();

    if (position >= fileSize) {
      return -1;
    }

    if (prefetchSize <= 0 || position < fileSize - prefetchSize) {
      return 0;
    }

    if (bufferCovers(position)) {
      // If we already fetched it locally for this stream, it's a guaranteed hit
      telemetry.recordMetric(Metric.FOOTER_PREFETCH_HIT, 1L, Collections.emptyMap());
      return serveFromBuffer(position, dst);
    }

    // AtomicBoolean serves as a mutable wrapper to signal intent clearly
    AtomicBoolean isMiss = new AtomicBoolean(false);
    ByteBuffer footer =
        cacheManager.getFooter(
            gcsItemId,
            itemId -> {
              isMiss.set(true);
              return loadFooter(source);
            });
    adoptFooterBuffer(footer);
    int bytesRead = serveFromBuffer(position, dst);

    if (!isMiss.get() && bytesRead > 0) {
      telemetry.recordMetric(Metric.FOOTER_CACHE_HIT, 1L, Collections.emptyMap());
    }

    return bytesRead;
  }

  private void adoptFooterBuffer(ByteBuffer footer) {
    localFooterBuffer = footer;
    localBufferStartPosition = fileSize - footer.limit();
  }

  private int readWithUnknownFileSize(
      long position, ByteBuffer dst, VectoredSeekableByteChannel source) throws IOException {
    long estimatedPrefetchSize = estimatePrefetchSize();
    if (!isLikelyFooterRead(position, dst, estimatedPrefetchSize)) {
      return 0;
    }

    // Read through the end of the object in one request: the response resolves the object size
    // and the bytes cover the footer. Attempted once per stream, so a misfire costs one over-read.
    speculativeReadAttempted = true;
    long startPosition = Math.max(0, position - estimatedPrefetchSize);
    ByteBuffer prefetchedBuffer = readToEndOfObject(startPosition, estimatedPrefetchSize, source);
    resolveFileSizeFromItemInfo(source);
    if (prefetchedBuffer == null) {
      return fileSize != -1 && position >= fileSize ? -1 : 0;
    }

    pendingSpeculativeBuffer = prefetchedBuffer;
    pendingSpeculativeStartPosition = startPosition;
    if (fileSize == -1) {
      return copyOut(prefetchedBuffer, startPosition, position, dst);
    }
    adoptPendingSpeculativeBuffer();
    if (position >= fileSize) {
      return -1;
    }

    return bufferCovers(position)
        ? serveFromBuffer(position, dst)
        : copyOut(prefetchedBuffer, startPosition, position, dst);
  }

  private void adoptPendingSpeculativeBuffer() throws IOException {
    ByteBuffer prefetchedBuffer = pendingSpeculativeBuffer;
    if (prefetchedBuffer == null) {
      return;
    }
    pendingSpeculativeBuffer = null;
    long startPosition = pendingSpeculativeStartPosition;
    if (prefetchSize <= 0 || startPosition + prefetchedBuffer.limit() != fileSize) {
      return;
    }

    // A network fetch happened regardless of whether the shared cache already held this footer.
    telemetry.recordMetric(Metric.FOOTER_CACHE_MISS, 1L, Collections.emptyMap());
    ByteBuffer canonicalFooter = toCanonicalFooter(startPosition, prefetchedBuffer);
    adoptFooterBuffer(
        canonicalFooter == null ? prefetchedBuffer : shareFooterIfGenerationKnown(canonicalFooter));
  }

  private ByteBuffer shareFooterIfGenerationKnown(ByteBuffer canonicalFooter) throws IOException {
    // Without a generation the cache key is the bare object name and could serve a stale footer
    // to a later generation of the same object, so keep the footer local to this stream.
    if (gcsItemId.getContentGeneration().isEmpty()) {
      return canonicalFooter;
    }
    return cacheManager.getFooter(gcsItemId, itemId -> canonicalFooter);
  }

  private static int copyOut(
      ByteBuffer buffer, long bufferStartPosition, long position, ByteBuffer dst) {
    long offset = position - bufferStartPosition;
    if (offset < 0 || offset >= buffer.limit()) {
      return 0;
    }
    ByteBuffer view = buffer.duplicate();
    view.position(Math.toIntExact(offset));

    int bytesToRead = Math.min(dst.remaining(), view.remaining());
    view.limit(view.position() + bytesToRead);
    dst.put(view);

    return bytesToRead;
  }

  @Nullable
  private ByteBuffer toCanonicalFooter(long startPosition, ByteBuffer eofAnchoredBuffer) {
    long canonicalStartPosition = fileSize - prefetchSize;
    if (startPosition > canonicalStartPosition) {
      return null;
    }

    ByteBuffer footerView = eofAnchoredBuffer.duplicate();
    footerView.position(Math.toIntExact(canonicalStartPosition - startPosition));
    ByteBuffer canonicalFooter = ByteBuffer.allocate(Math.toIntExact(prefetchSize));
    canonicalFooter.put(footerView);
    canonicalFooter.flip();

    return canonicalFooter;
  }

  @Nullable
  private ByteBuffer readToEndOfObject(
      long startPosition, long estimatedPrefetchSize, VectoredSeekableByteChannel source)
      throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(estimatedPrefetchSize * 2));
    long originalPosition = source.position();
    try {
      source.position(startPosition);
      while (buffer.hasRemaining()) {
        if (source.read(buffer) <= 0) {
          break;
        }
      }
    } finally {
      source.position(originalPosition);
    }
    buffer.flip();

    return buffer.limit() == 0 ? null : buffer;
  }

  private boolean isLikelyFooterRead(long position, ByteBuffer dst, long estimatedPrefetchSize) {
    // Parquet and ORC readers start with a small read near the end of the file.
    return readOptions.isFooterPrefetchEnabled()
        && dst.hasRemaining()
        && position > 0
        && dst.remaining() <= estimatedPrefetchSize;
  }

  private long estimatePrefetchSize() {
    return Math.min(readOptions.getFooterPrefetchSizeSmallFile(), MAX_SPECULATIVE_PREFETCH_SIZE);
  }

  private void resolveFileSizeExplicitly(VectoredSeekableByteChannel source) throws IOException {
    long size = source.size();
    resolveFileSizeFromItemInfo(source);
    if (fileSize == -1) {
      fileSize = size;
      prefetchSize = calculatePrefetchSize(fileSize, readOptions);
    }
  }

  private boolean bufferCovers(long position) {
    if (localFooterBuffer == null) {
      return false;
    }
    long offset = position - localBufferStartPosition;

    return offset >= 0 && offset < localFooterBuffer.limit();
  }

  private int serveFromBuffer(long position, ByteBuffer dst) {
    if (!bufferCovers(position)) {
      return 0;
    }
    ByteBuffer footerView = localFooterBuffer.duplicate();
    footerView.position(Math.toIntExact(position - localBufferStartPosition));

    int bytesToRead = Math.min(dst.remaining(), footerView.remaining());
    footerView.limit(footerView.position() + bytesToRead);
    dst.put(footerView);
    return bytesToRead;
  }

  private ByteBuffer loadFooter(VectoredSeekableByteChannel source) throws IOException {
    telemetry.recordMetric(Metric.FOOTER_CACHE_MISS, 1L, Collections.emptyMap());
    long startPosition = fileSize - prefetchSize;
    int bufferSize = Math.toIntExact(prefetchSize);
    ByteBuffer cacheBuffer = ByteBuffer.allocate(bufferSize);
    long originalPosition = source.position();
    try {
      source.position(startPosition);
      while (cacheBuffer.hasRemaining()) {
        if (source.read(cacheBuffer) == -1) {
          throw new IOException("Unexpected EOF encountered while reading footer.");
        }
      }
      cacheBuffer.flip();
      return cacheBuffer;
    } finally {
      source.position(originalPosition);
    }
  }

  private void resolveFileSizeFromItemInfo(VectoredSeekableByteChannel source) {
    GcsItemInfo itemInfo = source.getItemInfo();
    if (itemInfo == null || itemInfo.getSize() < 0) {
      return;
    }
    fileSize = itemInfo.getSize();
    prefetchSize = calculatePrefetchSize(fileSize, readOptions);
    if (itemInfo.getItemId().getContentGeneration().isPresent()) {
      this.gcsItemId = itemInfo.getItemId();
    }
  }

  @VisibleForTesting
  static long calculatePrefetchSize(long fileSize, GcsReadOptions readOptions) {
    if (!readOptions.isFooterPrefetchEnabled()) {
      return 0;
    }
    return fileSize > LARGE_FILE_SIZE_THRESHOLD
        ? Math.min(readOptions.getFooterPrefetchSizeLargeFile(), fileSize)
        : Math.min(readOptions.getFooterPrefetchSizeSmallFile(), fileSize);
  }
}
