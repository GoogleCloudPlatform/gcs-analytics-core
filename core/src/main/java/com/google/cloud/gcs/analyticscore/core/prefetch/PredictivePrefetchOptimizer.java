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

package com.google.cloud.gcs.analyticscore.core.prefetch;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.gcs.analyticscore.client.AnalyticsCacheManager;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.client.GcsPrefetchOptions;
import com.google.cloud.gcs.analyticscore.client.PrefetchBufferCache;
import com.google.cloud.gcs.analyticscore.client.VectoredSeekableByteChannel;
import com.google.cloud.gcs.analyticscore.core.optimizer.FormatOptimizer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

/**
 * A {@link FormatOptimizer} that speculatively fetches exact Parquet column chunks before the
 * engine asks for them.
 *
 * <p>Footer I/O is owned exclusively by {@code GcsFooterOptimizer}; this optimizer consumes the
 * footer published in {@link AnalyticsCacheManager}. If no footer is present in the global cache,
 * prefetching is skipped.
 *
 * <p>Speculation schedules exact column chunk and dictionary page byte ranges rather than fixed
 * blocks, avoiding read amplification on unprojected columns. Touching ranges are merged into one
 * span; ranges separated by a gap are fetched separately.
 *
 * <p>Instances are bound to a single stream and its reader thread.
 */
public final class PredictivePrefetchOptimizer implements FormatOptimizer {

  private static final String PARQUET_EXTENSION = ".parquet";
  private static final int MAX_CONCURRENT_PREFETCH_RANGES = 32;
  private static final String PREFETCH_DISABLED_IN_CACHE_MANAGER =
      "Predictive prefetching is disabled in the cache manager";

  private final GcsPrefetchOptions prefetchOptions;

  private GcsItemId itemId;
  private AnalyticsCacheManager cacheManager;
  private PrefetchBufferCache bufferCache;
  private PrefetchScheduler scheduler;
  @Nullable private RowGroupPrefetchTracker tracker;

  public PredictivePrefetchOptimizer(GcsPrefetchOptions prefetchOptions) {
    this.prefetchOptions = checkNotNull(prefetchOptions, "prefetchOptions cannot be null");
  }

  @Override
  public boolean isApplicable(GcsItemId itemId) {
    return prefetchOptions.isEnabled()
        && itemId
            .getObjectName()
            .map(name -> name.toLowerCase().endsWith(PARQUET_EXTENSION))
            .orElse(false);
  }

  @Override
  public void onOpen(GcsItemId itemId, AnalyticsCacheManager cacheManager) {
    this.bufferCache =
        cacheManager
            .getPrefetchBufferCache()
            .orElseThrow(() -> new IllegalStateException(PREFETCH_DISABLED_IN_CACHE_MANAGER));
    this.itemId = itemId;
    this.cacheManager = cacheManager;
    this.scheduler = new PrefetchScheduler(itemId, bufferCache, MAX_CONCURRENT_PREFETCH_RANGES);
  }

  @Override
  public int read(long position, ByteBuffer dst, VectoredSeekableByteChannel source)
      throws IOException {
    long fileSize = source.size();
    int requestedLength = dst.remaining();
    ensureTrackerLoaded();
    int servedBytes = bufferCache.serveFromCache(itemId, position, dst, tracker != null);
    if (tracker != null) {
      int readLength = servedBytes > 0 ? servedBytes : requestedLength;
      tracker.onSingleRead(
          position, readLength, ranges -> scheduler.schedule(source, ranges, fileSize));
    }
    return servedBytes;
  }

  @Override
  public List<GcsObjectRange> readVectored(
      List<GcsObjectRange> ranges,
      IntFunction<ByteBuffer> allocate,
      VectoredSeekableByteChannel source) {
    ensureTrackerLoaded();
    List<GcsObjectRange> unservedRanges =
        bufferCache.serveVectoredFromCache(itemId, ranges, allocate, source, tracker != null);
    if (tracker != null) {
      tracker.onVectoredRead(ranges);
    }
    return unservedRanges;
  }

  @Override
  public void onClose() {
    if (scheduler != null) {
      scheduler.close();
    }
  }

  private void ensureTrackerLoaded() {
    if (tracker != null) {
      return;
    }
    cacheManager
        .getFooter(itemId)
        .flatMap(ParquetFooterParser::parse)
        .ifPresent(
            layout ->
                tracker =
                    new RowGroupPrefetchTracker(
                        layout, cacheManager.getSchemaAccessHistory().get()));
  }
}
