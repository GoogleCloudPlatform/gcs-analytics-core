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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/** A {@link FormatOptimizer} that caches and serves GCS object footers (e.g., for Parquet). */
public class GcsFooterOptimizer implements FormatOptimizer {

  private static final int LARGE_FILE_SIZE_THRESHOLD = 1024 * 1024 * 1024; // 1 GB
  private static final Set<String> FOOTER_OPTIMIZABLE_EXTENSIONS = Set.of(".parquet", ".orc");

  /**
   * Upper bound on the window used by {@link #readWithUnknownFileSize}. The speculative read
   * fetches up to twice this amount, so it also caps the one-time over-read incurred when a
   * non-footer read trips {@link #isLikelyFooterRead}.
   */
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

  public GcsFooterOptimizer(GcsReadOptions readOptions, Telemetry telemetry) {
    this.readOptions = checkNotNull(readOptions, "readOptions cannot be null");
    this.telemetry = checkNotNull(telemetry, "telemetry cannot be null");
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return readOptions.isFooterPrefetchEnabled()
        && itemId
            .getObjectName()
            .map(
                name ->
                    FOOTER_OPTIMIZABLE_EXTENSIONS.stream()
                        .anyMatch(ext -> name.toLowerCase().endsWith(ext)))
            .orElse(false);
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

    if (prefetchSize <= 0 || position < fileSize - prefetchSize) {
      return 0;
    }

    if (position >= fileSize) {
      return -1;
    }

    if (bufferCovers(position)) {
      telemetry.recordMetric(Metric.FOOTER_PREFETCH_HIT, 1L, Collections.emptyMap());
      return serveFromBuffer(position, dst);
    }

    AtomicBoolean isMiss = new AtomicBoolean(false);
    ByteBuffer footer =
        cacheManager.getFooter(
            gcsItemId,
            itemId -> {
              isMiss.set(true);
              return loadFooter(source);
            });
    adoptFooterBuffer(footer);

    if (!isMiss.get()) {
      telemetry.recordMetric(Metric.FOOTER_CACHE_HIT, 1L, Collections.emptyMap());
    }

    return serveFromBuffer(position, dst);
  }

  private void adoptFooterBuffer(ByteBuffer footer) {
    localFooterBuffer = footer;
    localBufferStartPosition = fileSize - footer.limit();
  }

  /**
   * Handles a read while the object size is still unknown, without issuing a metadata request.
   *
   * <p>If the read looks like a footer read (see {@link #isLikelyFooterRead}), this reads from
   * {@code position - window} through the end of the object in a single request, where {@code
   * window} is the small-file footer prefetch size capped at {@link
   * #MAX_SPECULATIVE_PREFETCH_SIZE}. The read response lets the source resolve the object size,
   * after which the canonical footer is carved out of the prefetched bytes and cached. If the
   * window turns out not to reach the end of the object (the object is larger than {@code 2 *
   * window}), the requested bytes are still served from what was fetched and the next read falls
   * back to the regular path.
   *
   * <p>This is attempted at most once per stream; a misfire costs one bounded over-read.
   *
   * @return the number of bytes copied into {@code dst}, or 0 to defer to the delegate channel
   */
  private int readWithUnknownFileSize(
      long position, ByteBuffer dst, VectoredSeekableByteChannel source) throws IOException {
    long estimatedPrefetchSize = estimatePrefetchSize();
    if (!isLikelyFooterRead(position, dst, estimatedPrefetchSize)) {
      return 0;
    }

    speculativeReadAttempted = true;
    long startPosition = Math.max(0, position - estimatedPrefetchSize);
    ByteBuffer prefetchedBuffer = readToEndOfObject(startPosition, estimatedPrefetchSize, source);
    resolveFileSizeFromItemInfo(source);
    if (prefetchedBuffer == null) {
      return 0;
    }

    ByteBuffer canonicalFooter = toCanonicalFooter(startPosition, prefetchedBuffer);
    if (canonicalFooter == null) {
      return copyOut(prefetchedBuffer, startPosition, position, dst);
    }

    // A network fetch happened regardless of whether the shared cache already held this footer.
    telemetry.recordMetric(Metric.FOOTER_CACHE_MISS, 1L, Collections.emptyMap());
    adoptFooterBuffer(shareFooterIfGenerationKnown(canonicalFooter));

    return serveFromBuffer(position, dst);
  }

  /**
   * Publishes {@code canonicalFooter} to the shared cache and returns the cached instance, unless
   * the resolved item id carries no content generation. Without a generation the cache key would be
   * the bare bucket/object name and a later generation of the same object could be served a stale
   * footer, so in that case the footer is kept local to this stream.
   */
  private ByteBuffer shareFooterIfGenerationKnown(ByteBuffer canonicalFooter) throws IOException {
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
  private ByteBuffer toCanonicalFooter(long startPosition, ByteBuffer buffer) {
    if (fileSize == -1 || prefetchSize <= 0) {
      return null;
    }
    long canonicalStartPosition = fileSize - prefetchSize;
    if (startPosition > canonicalStartPosition || startPosition + buffer.limit() != fileSize) {
      return null;
    }

    ByteBuffer footerView = buffer.duplicate();
    footerView.position(Math.toIntExact(canonicalStartPosition - startPosition));
    ByteBuffer canonicalFooter = ByteBuffer.allocate(Math.toIntExact(prefetchSize));
    canonicalFooter.put(footerView);
    canonicalFooter.flip();

    return canonicalFooter;
  }

  /**
   * Reads up to {@code 2 * estimatedPrefetchSize} bytes starting at {@code startPosition}, stopping
   * early at end of object. A short result is safe: {@link #toCanonicalFooter} only produces a
   * cacheable footer when the bytes provably reach the end of the object, so a truncated read can
   * never poison the cache.
   */
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

  /**
   * Heuristic for "this read is probably for the footer" when the object size is unknown: footer
   * prefetching is enabled and the read is small (at most one window) and not at offset 0. Parquet
   * and ORC readers start by reading a few bytes near the end of the file, which this matches; a
   * small read elsewhere in the file costs a single bounded over-read and is not repeated.
   */
  private boolean isLikelyFooterRead(long position, ByteBuffer dst, long estimatedPrefetchSize) {
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
